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
 * ClassName: DwdTradePayScu
 * Package: com.jia.edu.realtime.app.dwd.db
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 11:39
 * @Version 1.0
 */
public class DwdTradePayScu {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度 和kafka分区数匹配
		env.setParallelism(4);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


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

		// TODO 3.获取kafka中topic_db主题的数据并生成水位线字段 然后筛选需要的表创建为kafka表
		String groupId = "dwd_trade_pay_suc_group";
		tableEnv.executeSql("create table topic_db(" +
				" `database` String ," +
				" `table` String , " +
				" `type` String ," +
				" `ts` String ," +
				" `data` map<String,String> , " +
				" `old` map<String ,String> ," +
				" `proc_time` as PROCTIME() , " +
				" row_time as to_timestamp(from_unixtime(cast(ts as bigint))) , " +
				" watermark for row_time as row_time" +
				")" + KafkaUtil.getKafkaDDL("topic_db1", groupId));

		Table paymentInfo = tableEnv.sqlQuery("select `data`['id'] id ," +
				" `data`['order_id'] order_id ," +
				" `data`['total_amount'] total_amount , " +
				" ts ," +
				" proc_time , " +
				" row_time " +
				"  from topic_db " +
				" where `table` = 'payment_info' and `type` = 'insert'  and `data`['payment_status']='1602' ");
		tableEnv.createTemporaryView("payment_info", paymentInfo);

		// TODO 4.从kafka主题中读取 dwd_trade_order_detail 主题数据 封装为flink sql表 同时指定水位线
		tableEnv.executeSql("create table dwd_trade_order_detail(" +
				" id String ," +
				" order_id String ," +
				" user_id String ," +
				" course_id String ," +
				" course_name String ," +
				" origin_amount String ," +
				" coupon_reduce String ," +
				" final_amount String ," +
				" date_id String ," +
				" session_id String ," +
				" province_id String ," +
				" source String ," +
				" ts String ," +
				" `proc_time` as PROCTIME() , " +
				" row_time as to_timestamp(from_unixtime(cast(ts as bigint))) , " +
				" watermark for row_time as row_time" +
				")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail", groupId));

		// TODO 5.将两张表通过 Interval join 关联到一起 获得支付成功数据
		Table intervalJoined = tableEnv.sqlQuery("select " +
				" p.id  ," +
				" p.order_id , " +
				" p.total_amount ," +
				" o.ts , " +
				" o.id detail_id , " +
				" o.user_id , " +
				" o.course_id , " +
				" o.course_name , " +
				" o.origin_amount , " +
				" o.coupon_reduce , " +
				" o.final_amount ," +
				" o.session_id , " +
				" o.province_id" +
				" from payment_info p , dwd_trade_order_detail o " +
				" where p.order_id = o.order_id " +
				" and o.row_time between p.row_time - INTERVAL '15' MINUTE and p.row_time + INTERVAL '5' SECOND ");
		tableEnv.createTemporaryView("joined_table", intervalJoined);

		// TODO 6.将数据写入kafka主题中
		tableEnv.executeSql("create table dwd_trade_pay_suc( " +
				" id String , " +
				" order_id String , " +
				" total_amount String ," +
				" ts String , " +
				" detail_id String , " +
				" user_id String ," +
				" course_id String ," +
				" course_name String , " +
				" origin_amount String ," +
				" coupon_reduce String ," +
				" final_amount String ," +
				" session_id String ," +
				" province_id String ," +
				" primary key (id) not enforced" +
				")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_suc"));
		tableEnv.executeSql("insert into dwd_trade_pay_suc select * from joined_table");

	}
}
