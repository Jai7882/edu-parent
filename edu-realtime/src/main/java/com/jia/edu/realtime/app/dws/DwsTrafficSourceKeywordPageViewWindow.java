package com.jia.edu.realtime.app.dws;

import com.jia.edu.realtime.common.EduConfig;
import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwsTrafiicSourceKeywordPageViewWindow
 * Package: com.jia.edu.realtime.app.dws
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 15:16
 * @Version 1.0
 */
public class DwsTrafficSourceKeywordPageViewWindow {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度 和kafka分区数匹配
		env.setParallelism(4);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
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
		String groupId = "dws_traffic_source_keyword_page_view_window_group";
		tableEnv.executeSql("create table page_log(" +
				" common map<String,String> , " +
				" page map<String ,String>," +
				" ts bigint , " +
				" row_time as to_timestamp(from_unixtime(cast(ts as bigint)/1000)) , " +
				" watermark for row_time as row_time" +
				")" + KafkaUtil.getKafkaDDL(topic, groupId));

		// TODO 4.过滤出搜索行为
		//	满足以下三个条件的即为搜索行为数据：
		//		page 字段下 item 字段不为 null；
		//		page 字段下 last_page_id 为 search；
		//		page 字段下 item_type 为 keyword
		Table searchKeyword = tableEnv.sqlQuery(" select `page`['item'] fullword, " +
				" row_time " +
				" from page_log " +
				" where `page`['item'] is not null  and `page`['item_type'] = 'keyword'");
		tableEnv.createTemporaryView("keyword", searchKeyword);

		// TODO 5.分组、开窗、聚合计算
		Table table = tableEnv.sqlQuery("SELECT " +
				" date_format(window_start , 'yyyy-MM-dd HH:mm:ss') stt, " +
				" date_format(window_end , 'yyyy-MM-dd HH:mm:ss') edt," +
				" fullword keyword, " +
				" date_format(window_start , 'yyyyMMdd' ) cur_date," +
				"count(*) keyword_count \n" +
				"  FROM TABLE(\n" +
				"    TUMBLE(TABLE keyword, DESCRIPTOR(row_time), INTERVAL '10' seconds))\n" +
				"  GROUP BY window_start, window_end , fullword");
		tableEnv.createTemporaryView("grouped", table);

		// TODO 6.将聚合数据写到Doris
		tableEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
				"    stt STRING,\n" +
				"    edt STRING,\n" +
				"    keyword STRING,\n" +
				"	 cur_date string, " +
				"    keyword_count bigint\n" +
				"    ) \n" +
				"    WITH (\n" +
				"      'connector' = 'doris',\n" +
				"      'fenodes' = '" + EduConfig.DORIS_FE + "',\n" +
				"      'table.identifier' = '" + EduConfig.DORIS_DB + ".dws_traffic_source_keyword_page_view_window',\n" +
				"      'username' = 'root',\n" +
				"      'password' = '000000',\n" +
				"  'sink.properties.format' = 'json', " +
				"  'sink.properties.read_json_by_line' = 'true', " +
				"  'sink.buffer-count' = '4', " +
				"  'sink.buffer-size' = '4086'," +
				"  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
				")  ");
		// 写入doris中
		tableEnv.executeSql("insert into dws_traffic_source_keyword_page_view_window select * from grouped");
	}

}
