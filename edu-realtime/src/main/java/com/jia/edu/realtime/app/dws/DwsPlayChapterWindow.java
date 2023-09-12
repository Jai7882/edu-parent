package com.jia.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: DwsPlayChapterWindow
 * Package: com.jia.edu.realtime.app.dws
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 16:45
 * @Version 1.0
 */
public class DwsPlayChapterWindow {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 5678);
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
		String topic = "dwd_traffic_app_video_log";
		String groupId = "dws_play_chapter_window_group";
		KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
		DataStreamSource<String> appVideoDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "appVideoSource");

		// TODO 4.转换数据类型 并过滤空值 方便后续操作
		SingleOutputStreamOperator<JSONObject> processed = appVideoDs.process(new ProcessFunction<String, JSONObject>() {
			@Override
			public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
				if (value != null) {
					JSONObject jsonObject = JSON.parseObject(value);
					out.collect(jsonObject);
				}
			}
		});

		// TODO 5.生成水位线
		SingleOutputStreamOperator<JSONObject> withWatermarkDs
				= processed.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
				.withTimestampAssigner((e,ts)->e.getLong("ts")));

		// TODO 6.根据sessionId进行分组


	}
}
