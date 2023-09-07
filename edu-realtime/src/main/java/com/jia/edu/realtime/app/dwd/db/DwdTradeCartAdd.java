package com.jia.edu.realtime.app.dwd.db;

import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeCartAdd
 * Package: com.jia.edu.realtime.app.dwd.db
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 10:17
 * @Version 1.0
 */
public class DwdTradeCartAdd {

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

		// TODO 3.获取kafka中topic_db主题的数据并筛选需要的表创建为kafka表
		String groupId = "dwd_trade_cart_add_group";
		tableEnv.executeSql(KafkaUtil.getTopiDbDDL(groupId));
		Table table = tableEnv.sqlQuery("select `data`['id'] id , " +
				" `data`['user_id'] user_id , " +
				" `data`['course_id'] course_id ," +
				" `data`['course_name'] course_name," +
				" `data`['cart_price'] cart_price ," +
				" `data`['session_id'] session_id ," +
				" ts " +
				" from topic_db where `table` = 'cart_info' and `type` = 'insert' ");
		tableEnv.createTemporaryView("cart_info" , table);

		// TODO 4.将加购表写到kafka的主题中
		// 创建动态表和要写入的主题进行映射
		tableEnv.executeSql("create table dwd_trade_cart_add(" +
				" id String ," +
				" user_id String ," +
				" course_id String ," +
				" course_name String , " +
				" cart_price String ," +
				" session_id String ," +
				" ts String ," +
				" primary key (id) not enforced " +
				")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add"));
		tableEnv.executeSql("insert into dwd_trade_cart_add select * from cart_info");

	}
}
