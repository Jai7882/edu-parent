package com.jia.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.app.function.BeanToJsonStrFunction;
import com.jia.edu.realtime.bean.TrafficPageViewBean;
import com.jia.edu.realtime.util.DateFormatUtil;
import com.jia.edu.realtime.util.DorisUtil;
import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: DwsTrafficScIsNewPageViewWindow
 * Package: com.jia.edu.realtime.app.dws
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 21:14
 * @Version 1.0
 */
public class DwsTrafficScIsNewPageViewWindow {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度 和kafka分区数匹配
		env.setParallelism(4);
		// 设置 ttl
//		tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

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

		// TODO 3.从页面日志中读取数据  创建动态表 指定Watermark生成策略以及提取事件时间字段
		String topic = "dwd_traffic_page_log";
		String groupId = "dws_traffic_sc_is_new_page_view_window_group";
		KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
		DataStreamSource<String> pageDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
//		pageDs.print();

		// TODO 4.对数据进行转化方便后续操作 jsonStr->jsonObject
		SingleOutputStreamOperator<JSONObject> transformed = pageDs.map(JSON::parseObject);

		// TODO 5.按照mid进行分组
		KeyedStream<JSONObject, String> keyed = transformed.keyBy(e -> e.getJSONObject("common").getString("mid"));

		// TODO 6.处理流中数据  相当于WordCount的计数   jsonObj->实体类对象
		SingleOutputStreamOperator<TrafficPageViewBean> processed = keyed.process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
			// 用于判断是否为独立访客
			private ValueState<String> lastVisitDateState;
			// 用于维护是否需要将跳出数-1
			private ValueState<Boolean> lastOutDateState;

			// 状态初始化
			@Override
			public void open(Configuration parameters) throws Exception {
				ValueStateDescriptor<String> stateProperties = new ValueStateDescriptor<String>("valueState", String.class);
				// 设置TTL 状态过期时间为 1天
				stateProperties.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
				lastVisitDateState = getRuntimeContext().getState(stateProperties);

				ValueStateDescriptor<Boolean> stateProperty = new ValueStateDescriptor<Boolean>("valueState2", Boolean.class);
				stateProperties.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
				lastOutDateState = getRuntimeContext().getState(stateProperty);
			}

			@Override
			public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
				JSONObject common = value.getJSONObject("common");
				String sc = common.getString("sc");
				String isNew = common.getString("is_new");
				JSONObject page = value.getJSONObject("page");
				String lastPageId = page.getString("last_page_id");
				Long ts = value.getLong("ts");
				String curDate = DateFormatUtil.toDate(ts);
				Boolean ifOut = lastOutDateState.value();
				// 有可能出现日志到达时间乱序的情况 所以当乱序发生 last_page_id不为空 的数据提前到来 会出现空指针异常 所以为了防止这样的结果
				// 通过判断ifOut是否为null 如果为null 赋予默认值为false
				if (ifOut == null) {
					ifOut = false;
				}
				String lastVisit = lastVisitDateState.value();
				Long uvCt = 0L;
				Long svCt = 0L;
				Long soCt = 0L;
				Long pvCt = 1L;
				Long durSum = page.getLong("during_time");

				// 判断状态中上次访问日期不为空 并且 状态中的日期与当前日期不相同 说明为独立访客 uvCt记为1 并将当前日期更新到状态中
				if (StringUtils.isEmpty(lastVisit)) {
					uvCt = 1L;
					lastVisitDateState.update(curDate);
				}else if (StringUtils.isNotEmpty(lastVisit) && !curDate.equals(lastVisit)){
					uvCt = 1L;
					lastVisitDateState.update(curDate);
				}

				// 判断上一个页面id是否为空 如果为空 说明为新会话 sessionCount++
				// 并将会话跳出数记为1 并更新另一个状态中的会话是否跳出为true
				// 当上次页面id不为空 需先从状态中取得该状态 是否跳出 如果被标记为true 说明会话并未跳出 修复跳出数 将跳出数-1
				// 并更新状态为false 然后当该会话再次产生日志 会判断状态的值判断会话是否跳出 由于上次修复过跳出数 并且状态为false
				// 该条记录则不会对跳出数进行修改
				if (StringUtils.isEmpty(lastPageId)) {
					soCt = 1L;
					svCt = 1L;
					ifOut = true;
				} else if (ifOut) {
					soCt = -1L;
					ifOut = false;
				}
				lastOutDateState.update(ifOut);

				// 最后将数据向下游传递
				out.collect(new TrafficPageViewBean(
						"",
						"",
						sc,
						isNew,
						"",
						uvCt,
						svCt,
						soCt,
						pvCt,
						durSum,
						ts
				));
			}
		});

		// TODO 7.指定Watermark的生成策略以及提取事件时间字段
		SingleOutputStreamOperator<TrafficPageViewBean> withWatermark
				= processed.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
				.withTimestampAssigner((e, ts) -> e.getTs()));

		// TODO 8.按照统计的维度进行分组
		KeyedStream<TrafficPageViewBean, Tuple2<String, String>> dimKeyed = withWatermark.keyBy(
				new KeySelector<TrafficPageViewBean, Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> getKey(TrafficPageViewBean value) throws Exception {
						return Tuple2.of(value.getSc(), value.getIsNew());
					}
				}
		);

		// TODO 9.开窗
		WindowedStream<TrafficPageViewBean, Tuple2<String, String>, TimeWindow> windowed
				= dimKeyed.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

		// TODO 10.聚合计算
		SingleOutputStreamOperator<TrafficPageViewBean> reduced = windowed.reduce((e1, e2) -> {
			e1.setUvCt(e1.getUvCt() + e2.getUvCt());
			e1.setSvCt(e1.getSvCt() + e2.getSvCt());
			e1.setSoCt(e1.getSoCt() + e2.getSoCt());
			e1.setPvCt(e1.getPvCt() + e2.getPvCt());
			e1.setDurSum(e1.getDurSum() + e2.getDurSum());
			return e1;
		}, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple2<String, String>, TimeWindow>() {
			@Override
			public void apply(Tuple2<String, String> stringStringTuple2, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
				String stt = DateFormatUtil.toYmdHms(window.getStart());
				String edt = DateFormatUtil.toYmdHms(window.getEnd());
				String curDate = DateFormatUtil.toDate(window.getStart());
				for (TrafficPageViewBean trafficPageViewBean : input) {
					trafficPageViewBean.setStt(stt);
					trafficPageViewBean.setEdt(edt);
					trafficPageViewBean.setCurDate(curDate);
					out.collect(trafficPageViewBean);
				}
			}
		});

		// TODO 11.将聚合结果写入Doris
		reduced.map(new BeanToJsonStrFunction<TrafficPageViewBean>())
				.sinkTo(DorisUtil.getDorisSink("dws_traffic_sc_is_new_page_view_window"));

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
