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
 * ClassName: DwdUserUserRegister
 * Package: com.jia.edu.realtime.app.dwd.db
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 14:30
 * @Version 1.0
 */
public class DwdUserUserRegister {

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

		// TODO 3.读取 topic_db 主题中的数据 并获取需要的表  user_info
		String groupId = "dwd_user_user_register_group";
		tableEnv.executeSql(KafkaUtil.getTopiDbDDL(groupId));
		Table userInfo = tableEnv.sqlQuery("select `data`['id'] id , " +
				" `data`['login_name'] login_name , " +
				" `data`['user_level'] user_level , " +
				" `data`['create_time'] create_time ," +
				" ts " +
				" from topic_db  " +
				" where `table` = 'user_info' " +
				" and `type` = 'insert' ");
		tableEnv.createTemporaryView("user_info",userInfo);

		// TODO 4.将数据写入Kafka主题中
		tableEnv.executeSql("create table dwd_user_user_register(" +
				" id String ," +
				" login_name String ," +
				" user_level String ," +
				" create_time String ," +
				" ts String , " +
				" primary key (id) not enforced" +
				")" + KafkaUtil.getUpsertKafkaDDL("dwd_user_user_register"));
		// 写入
		tableEnv.executeSql("insert into dwd_user_user_register select * from user_info");

	}
}
