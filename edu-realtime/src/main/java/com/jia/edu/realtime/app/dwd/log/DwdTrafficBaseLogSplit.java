package com.jia.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.util.DateFormatUtil;
import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ClassName: DwdTrafficBaseLogSplit
 * Package: com.jia.edu.realtime.app.dwd.log
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 19:26
 * @Version 1.0
 */
public class DwdTrafficBaseLogSplit {

	public static void main(String[] args) {

		// TODO 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度 和kafka分区数匹配
		env.setParallelism(4);

		// TODO 2.检查点相关配置
		// 开启检查点
		env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
		// 设置检查点超时时间
//		env.getCheckpointConfig().setCheckpointTimeout(5000L);
//		// 设置任务失败保留检查点
//		env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//		// 设置检查点最小间隔时间
//		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//		// 设置状态后端
//		env.setStateBackend(new HashMapStateBackend());
//		// 设置检查点重启策略
		env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(3L), Time.seconds(3L)));
//		// 设置检查点存储在hdfs中的位置
//		env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/edu/ck");
//		// 设置Hadoop操作用户
		System.setProperty("HADOOP_USER_NAME", "jia");

		// TODO 3.从Kafka主题中读取数据
		String topic = "topic_log1";
		String groupId = "dwd_traffic_base_log_split_group";
		KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
		DataStreamSource<String> topicLogDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
		// 转换为JsonObject类型方便后续操作
		OutputTag<String> dirty = new OutputTag<>("dirty", Types.STRING);
		SingleOutputStreamOperator<JSONObject> processed = topicLogDs.process(
				new ProcessFunction<String, JSONObject>() {
					@Override
					public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
						try {
							JSONObject jsonObject = JSON.parseObject(value);
							out.collect(jsonObject);
						} catch (Exception e) {
							// 发生异常说明不是一个标准json字符串 属于脏数据 输出到侧流
							ctx.output(dirty, value);
						}
					}
				}
		);

		// TODO 5.将侧输出流中的脏数据输出到Kafka主题
		// 脏数据
		String dirtyTopic = "dirty_topic";
		SideOutputDataStream<String> dirtyDs = processed.getSideOutput(dirty);
		KafkaSink<String> kafkaSink = KafkaUtil.getKafkaSink(dirtyTopic);
		dirtyDs.sinkTo(kafkaSink);

		// TODO 6.根据设备id进行分组
		KeyedStream<JSONObject, String> keyed = processed.keyBy(e -> e.getJSONObject("common").getString("mid"));

		// TODO 7.新老访客标记修复
		SingleOutputStreamOperator<JSONObject> fixed = keyed.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
			private ValueState<String> valueState;

			@Override
			public void open(Configuration parameters) throws Exception {
				// 初始化状态
				ValueStateDescriptor<String> stateProperties = new ValueStateDescriptor<>("valueState", String.class);
				valueState = getRuntimeContext().getState(stateProperties);
			}

			@Override
			public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
				// 修复标记 通过判断is_new字段与状态
				String isNew = value.getJSONObject("common").getString("is_new");
				String lastLoginDate = valueState.value();
				Long ts = value.getLong("ts");
				String curDate = DateFormatUtil.toDate(ts);
				// 如果is_new的值为1
				//  如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
				//  如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
				//  如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；
				if ("1".equals(isNew)) {
					if (StringUtils.isNotEmpty(lastLoginDate) && !lastLoginDate.equals(curDate)) {
						// 状态里有值并且不为当前日期 进行修复状态
						isNew = "0";
						value.getJSONObject("common").put("is_new",isNew);
					} else {
						// 是新用户 将当前访问日期更新到状态中
						valueState.update(curDate);
					}
				} else {
					//如果 is_new 的值为 0
					//  如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。
					//  当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，
					//  只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日
					if (StringUtils.isEmpty(lastLoginDate)) {
						valueState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000));
					}
					//  如果键控状态不为 null，说明程序已经维护了首次访问日期，不做操作。

				}
				out.collect(value);
			}
		});

		// TODO 8.分流 错误日志 -> 错误侧输出流  启动日志 -> 启动侧输出流 曝光日志 -> 曝光侧输出流 动作日志 -> 动作侧输出流 页面日志 -> 页面侧输出流
		OutputTag<String> errTag = new OutputTag<>("err_tag", Types.STRING);
		OutputTag<String> actionTag = new OutputTag<>("action_tag", Types.STRING);
		OutputTag<String> appVideoTag = new OutputTag<>("app_video_tag", Types.STRING);
		OutputTag<String> startTag = new OutputTag<>("start_tag", Types.STRING);
		OutputTag<String> displayTag = new OutputTag<>("display_tag", Types.STRING);
		SingleOutputStreamOperator<String> pageDs = fixed.process(
				new ProcessFunction<JSONObject, String>() {
					@Override
					public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
						// 所有日志数据都可能拥有 err 字段，所有首先获取 err 字段，
						// 如果返回值不为 null 则将整条日志数据发送到错误侧输出流。然后删掉 JSONObject 中的 err 字段及对应值
						// 错误日志处理
						JSONObject err = value.getJSONObject("err");
						if (err != null){
							ctx.output(errTag,value.toJSONString());
							value.remove("err");
						}

						// 播放日志处理
						JSONObject appVideo = value.getJSONObject("appVideo");
						if (appVideo != null){
							ctx.output(appVideoTag,value.toJSONString());
							value.remove("appVideo");
						}
						// 判断是否有 start 字段，如果有则说明数据为启动日志，将其发送到启动侧输出流；如果没有则说明为页面日志，进行下一步
						JSONObject start = value.getJSONObject("start");
						if (start != null){
							ctx.output(startTag,value.toJSONString());
						}else {
							// 页面日志必然有 page 字段、 common 字段和 ts 字段，获取它们的值，ts 封装为包装类 Long，其余两个字段的值封装为 JSONObject
							// 如果不是启动日志就是页面日志

							JSONObject page = value.getJSONObject("page");
							JSONObject common = value.getJSONObject("common");
							JSONArray displays = value.getJSONArray("displays");
							Long ts = value.getLong("ts");
							// 判断是否有 displays 字段，如果有，将其值封装为 JSONArray，遍历该数组，依次获取每个元素（记为 display），封装为JSONObject。
							// 创建一个空的 JSONObject，将 display、common、page和 ts 添加到该对象中，获得处理好的曝光数据，发送到曝光侧输出流。
							// 动作日志的处理与曝光日志相同
							if (displays != null && displays.size()>0){
								for (int i = 0; i < displays.size(); i++) {
									JSONObject jsonObject = new JSONObject();
									JSONObject display = displays.getJSONObject(i);
									jsonObject.put("common",common);
									jsonObject.put("page",page);
									jsonObject.put("display",display);
									jsonObject.put("ts",ts);
									ctx.output(displayTag,jsonObject.toJSONString());
								}
								value.remove("displays");
							}

							// 动作日志处理
							JSONArray actions = value.getJSONArray("actions");
							if (actions != null && actions.size()>0){
								for (int i = 0; i < actions.size(); i++) {
									JSONObject jsonObject = new JSONObject();
									JSONObject action = actions.getJSONObject(i);
									jsonObject.put("common",common);
									jsonObject.put("page",page);
									jsonObject.put("action",action);
									ctx.output(actionTag,jsonObject.toJSONString());
					}
								value.remove("actions");
							}
							out.collect(value.toJSONString());
						}
					}
				}
		);

		SideOutputDataStream<String> actionDs = pageDs.getSideOutput(actionTag);
		SideOutputDataStream<String> errDs = pageDs.getSideOutput(errTag);
		SideOutputDataStream<String> appVideoDs = pageDs.getSideOutput(appVideoTag);
		SideOutputDataStream<String> displayDs = pageDs.getSideOutput(displayTag);
		SideOutputDataStream<String> startDs = pageDs.getSideOutput(startTag);


		// TODO 9.将不同的流输出到不同的主题中
		String actionTopic = "dwd_traffic_action_log";
		String errTopic = "dwd_traffic_err_log";
		String startTopic = "dwd_traffic_start_log";
		String appVideoTopic = "dwd_traffic_app_video_log";
		String displayTopic = "dwd_traffic_display_log";
		String pageTopic = "dwd_traffic_page_log";

		pageDs.sinkTo(KafkaUtil.getKafkaSink(pageTopic));
		actionDs.sinkTo(KafkaUtil.getKafkaSink(actionTopic));
		errDs.sinkTo(KafkaUtil.getKafkaSink(errTopic));
		appVideoDs.sinkTo(KafkaUtil.getKafkaSink(appVideoTopic));
		displayDs.sinkTo(KafkaUtil.getKafkaSink(displayTopic));
		startDs.sinkTo(KafkaUtil.getKafkaSink(startTopic));

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
