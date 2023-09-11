package com.jia.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.app.function.BeanToJsonStrFunction;
import com.jia.edu.realtime.app.function.DimAsyncFunction;
import com.jia.edu.realtime.bean.CourseOrderBean;
import com.jia.edu.realtime.util.DateFormatUtil;
import com.jia.edu.realtime.util.DorisUtil;
import com.jia.edu.realtime.util.KafkaUtil;
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
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsCourseCategorySubjectCourseWindow
 * Package: com.jia.edu.realtime.app.dws
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/8 9:49
 * @Version 1.0
 */
public class DwsTradeScArWindow {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port",5678);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
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
		String topic = "dwd_trade_order_detail";
		String groupId = "dws_trade_sc_ar_window_group";
		KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
		DataStreamSource<String> orderDetailDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
//		orderDetailDs.print();
		// TODO 4.过滤null值并转换数据类型
		SingleOutputStreamOperator<JSONObject> processed = orderDetailDs.process(
				new ProcessFunction<String, JSONObject>() {
					@Override
					public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
						if (value != null) {
							JSONObject jsonObject = JSON.parseObject(value);
							out.collect(jsonObject);
						}
					}
				}
		);

		// TODO 5.生成水位线
		SingleOutputStreamOperator<JSONObject> withWatermarkDs = processed.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
				.withTimestampAssigner((e, ts) -> e.getLong("ts") * 1000));

		// TODO 6.按照用户 id 分组 来统计下单用户人数
		KeyedStream<JSONObject, String> keyed = withWatermarkDs.keyBy(e -> e.getString("user_id"));

		// TODO 7.统计独立下单用户 下单数 下单金额等
		SingleOutputStreamOperator<CourseOrderBean> process = keyed.process(
				new KeyedProcessFunction<String, JSONObject, CourseOrderBean>() {

					private ValueState<String> lastOrderDateState;

					@Override
					public void open(Configuration parameters) throws Exception {
						ValueStateDescriptor<String> stateProperties = new ValueStateDescriptor<String>("state", String.class);
						stateProperties.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
						lastOrderDateState = getRuntimeContext().getState(stateProperties);
					}

					@Override
					public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, CourseOrderBean>.Context ctx, Collector<CourseOrderBean> out) throws Exception {
						Long orderUserCt = 0L;
						String lastOrderDate = lastOrderDateState.value();
						String sc = value.getString("source");
						Long ts = value.getLong("ts") * 1000L;
						String curDate = DateFormatUtil.toDate(ts);
						if (lastOrderDate == null) {
							orderUserCt = 1L;
							lastOrderDateState.update(curDate);
						} else if (!lastOrderDate.equals(curDate)) {
							orderUserCt = 1L;
							lastOrderDateState.update(curDate);
						}
						Long orderCt = 1L;

						out.collect(CourseOrderBean.builder()
								.orderUserCt(orderUserCt)
								.orderCt(orderCt)
								.ts(ts)
								.sc(sc)
								.provinceId(value.getString("province_id"))
								.orderAmount(value.getBigDecimal("final_amount"))
								.build()
						);
					}
				}
		);
		// 分组
		KeyedStream<CourseOrderBean, Tuple2<String, String>> keyedDs = process.keyBy(new KeySelector<CourseOrderBean,  Tuple2<String, String>>() {
			@Override
			public Tuple2<String,  String> getKey(CourseOrderBean value) throws Exception {
				Tuple2< String, String> t2 = new Tuple2<>(value.getSc(),
						value.getProvinceId());
				return t2;
			}
		});

		// TODO 8.开窗
		WindowedStream<CourseOrderBean, Tuple2<String, String>, TimeWindow> windowed
				= keyedDs.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

		// TODO 9.聚合
		SingleOutputStreamOperator<CourseOrderBean> reduced = windowed.reduce((e1, e2) -> {
			e1.setOrderCt(e1.getOrderCt() + e2.getOrderCt());
			e1.setOrderUserCt(e1.getOrderUserCt() + e2.getOrderUserCt());
			e1.setOrderAmount(e1.getOrderAmount().add(e2.getOrderAmount()));
			return e1;
		}, new ProcessWindowFunction<CourseOrderBean, CourseOrderBean, Tuple2<String, String>, TimeWindow>() {
			@Override
			public void process(Tuple2<String,  String> stringStringStringStringStringTuple5, ProcessWindowFunction<CourseOrderBean, CourseOrderBean, Tuple2<String,  String>, TimeWindow>.Context context, Iterable<CourseOrderBean> elements, Collector<CourseOrderBean> out) throws Exception {
				String stt = DateFormatUtil.toYmdHms(context.window().getStart());
				String curDate = DateFormatUtil.toDate(context.window().getStart());
				String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
				for (CourseOrderBean element : elements) {
					element.setStt(stt);
					element.setEdt(edt);
					element.setCurDate(curDate);
					out.collect(element);
				}
			}
		});
		SingleOutputStreamOperator<CourseOrderBean> joined = AsyncDataStream.unorderedWait(
				reduced,
				new DimAsyncFunction<CourseOrderBean>("dim_base_source") {
					@Override
					public void join(CourseOrderBean obj, JSONObject dimInfoJsonObj) {
						obj.setSourceName(dimInfoJsonObj.getString("source_site"));
					}

					@Override
					public String getKey(CourseOrderBean obj) {
						return obj.getSc();
					}
				}
				, 60, TimeUnit.SECONDS
		);

		SingleOutputStreamOperator<CourseOrderBean> completed = AsyncDataStream.unorderedWait(
				joined,
				new DimAsyncFunction<CourseOrderBean>("dim_base_province") {
					@Override
					public void join(CourseOrderBean obj, JSONObject dimInfoJsonObj) {
						obj.setProvinceName(dimInfoJsonObj.getString("name"));
					}

					@Override
					public String getKey(CourseOrderBean obj) {
						return obj.getProvinceId();
					}
				}
				, 60, TimeUnit.SECONDS
		);

		completed.map(new BeanToJsonStrFunction<>())
				.sinkTo(DorisUtil.getDorisSink("dws_trade_sc_ar_window"));

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
