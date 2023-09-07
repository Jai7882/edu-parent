package com.jia.edu.realtime.app.dwd.db;

import com.jia.edu.realtime.util.HbaseUtil;
import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdInteractionScoreInfo
 * Package: com.jia.edu.realtime.app.dwd.log.db
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 0:42
 * @Version 1.0
 * <p>
 * 互动域评分事务事实表
 */
public class DwdInteractionScoreInfo {

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
		String groupId = "dwd_interaction_score_info_group";
		tableEnv.executeSql(KafkaUtil.getTopiDbDDL(groupId));
		Table table = tableEnv.sqlQuery("select " +
				" `data`['id'] id ," +
				" `data`['user_id'] user_id , " +
				" `data`['course_id'] course_id," +
				" `data`['review_stars'] review_stars ," +
				"  ts, " +
				"   proc_time" +
				" from topic_db " +
				" where `table` = 'review_info'  and `type` = 'insert' ");
		tableEnv.createTemporaryView("review_info", table);

		// TODO 4.从Hbase中获取对应维度表并创建动态表 Hbase lookup join 表名 course_info
		tableEnv.executeSql(HbaseUtil.getCourseInfoLookUpDDL());

		// TODO 5.将评论表和字典表进行关联
		Table joined = tableEnv.sqlQuery("select r.id , " +
				" user_id ," +
				" course_id , " +
				" c.course_name ," +
				" review_stars," +
				" ts " +
				" from review_info r join course_info FOR SYSTEM_TIME AS OF r.proc_time AS c " +
				" on r.course_id = c.id");
		tableEnv.createTemporaryView("joined_table" , joined);

		// TODO 6.将关联之后的数据写到kafka主题中
		// 创建动态表和要写入的kafka主题进行映射
		tableEnv.executeSql("create table dwd_interaction_score_info(" +
				" id String ," +
				" user_id String ," +
				" course_id String ," +
				" course_name String ," +
				" review_stars String ," +
				" ts String ," +
				" primary key (id) not enforced)" + KafkaUtil.getUpsertKafkaDDL("dwd_interaction_score_info"));

		// TODO 7.将数据插入到主题中
		tableEnv.executeSql("insert into dwd_interaction_score_info select * from joined_table");
		
	}
}
