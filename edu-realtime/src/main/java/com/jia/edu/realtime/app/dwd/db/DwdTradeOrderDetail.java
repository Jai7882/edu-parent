package com.jia.edu.realtime.app.dwd.db;

import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderDetail
 * Package: com.jia.edu.realtime.app.dwd.db
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 10:56
 * @Version 1.0
 */
public class DwdTradeOrderDetail {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度 和kafka分区数匹配
		env.setParallelism(4);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		// 设置 ttl
		tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

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

		// TODO 3.获取kafka中topic_db主题的数据并筛选需要的表创建为kafka表
		String groupId = "dwd_trade_order_detail_group";
		tableEnv.executeSql(KafkaUtil.getTopiDbDDL(groupId));
		Table orderDetail = tableEnv.sqlQuery("select `data`['id'] id , " +
				" `data`['course_id'] course_id ," +
				" `data`['course_name'] course_name," +
				" `data`['order_id'] order_id," +
				" `data`['user_id'] user_id," +
				" `data`['origin_amount'] origin_amount ," +
				" `data`['coupon_reduce'] coupon_reduce ," +
				" `data`['final_amount'] final_amount ," +
				" `data`['create_time'] create_time," +
				" ts " +
				" from topic_db where `table` = 'order_detail' and `type` = 'insert' ");
		tableEnv.createTemporaryView("order_detail" , orderDetail);

		// 获得 dwd_traffic_page_log 数据 为了获得sc
		String topic = "dwd_traffic_page_log";
		tableEnv.executeSql("create table dwd_traffic_page_log(" +
				" `common` map<String,String>  , " +
				" `page` map<String,String> , " +
				" ts String " +
				")" + KafkaUtil.getKafkaDDL(topic,groupId));

		// 只获取关心的字段 sid 和 sc
		Table scSid = tableEnv.sqlQuery("select `common`['sc'] sc , " +
				" `common`['sid'] sid  from dwd_traffic_page_log where `page`['page_id'] = 'order' ");
		tableEnv.createTemporaryView("sc_sid" , scSid);

		Table orderInfo = tableEnv.sqlQuery("select `data`['id'] id , " +
				" `data`['user_id'] user_id ," +
				" `data`['origin_amount'] origin_amount," +
				" `data`['coupon_reduce'] coupon_reduce," +
				" `data`['final_amount'] final_amount," +
				" `data`['order_status'] order_status ," +
				" `data`['session_id'] session_id ," +
				" `data`['province_id'] province_id ," +
				" ts " +
				" from topic_db where `table` = 'order_info' and `type` = 'insert' ");
		tableEnv.createTemporaryView("order_info" , orderInfo);

		// TODO 4.将订单表和订单明细表进行关联 由于订单明细表粒度更细 所以以订单明细表作为主表 还有来源 sessionId 表关联获取sc字段
		Table joined = tableEnv.sqlQuery("select  d.id," +
				" d.order_id , " +
				" d.user_id , " +
				" d.course_id , " +
				" d.course_name ," +
				" d.origin_amount , " +
				" d.coupon_reduce , " +
				" d.final_amount , " +
				" date_format(d.create_time , 'yyyy-MM-dd') date_id ," +
				" d.create_time , " +
				" i.session_id , " +
				" i.province_id , " +
				" s.sc , " +
				" d.ts" +
				" from order_detail d join order_info i " +
				" on d.order_id = i.id" +
				" join sc_sid s " +
				" on i.session_id = s.sid ");
		tableEnv.createTemporaryView("joined_table" , joined);

		// TODO 5.将关联的结果写到kafka主题
		tableEnv.executeSql("create table dwd_trade_order_detail(" +
				" id String ," +
				" order_id String ," +
				" user_id String , " +
				" course_id String ," +
				" course_name String ," +
				" origin_amount String , " +
				" coupon_reduce String ," +
				" final_amount String ," +
				" date_id String ," +
				" create_time String ," +
				" session_id String ," +
				" province_id String ," +
				" `source` String ," +
				" ts String ," +
				" primary key (id) not enforced)" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail"));
		tableEnv.executeSql("insert into dwd_trade_order_detail select * from joined_table");

	}

}
