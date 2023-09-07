package com.jia.edu.realtime.app.dwd.db;

import com.jia.edu.realtime.util.HbaseUtil;
import com.jia.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdExamExamPaper
 * Package: com.jia.edu.realtime.app.dwd.db
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 13:28
 * @Version 1.0
 */
public class DwdExamExamPaperQuestion {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度 和kafka分区数匹配
		env.setParallelism(4);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		// 设置 ttl
		tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

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

		// TODO 3.读取 topic_db 主题中的数据 并获取需要的表  test_exam_question
		String groupId = "dwd_exam_exam_paper_question_group";
		tableEnv.executeSql(KafkaUtil.getTopiDbDDL(groupId));
		Table testExamQuestion = tableEnv.sqlQuery("select `data`['id'] id , " +
				" `data`['exam_id'] exam_id , " +
				" `data`['paper_id'] paper_id , " +
				" `data`['question_id'] question_id , " +
				" `data`['user_id'] user_id , " +
				" `data`['is_correct'] is_correct " +
				" from topic_db  " +
				" where `table` = 'test_exam_question' " +
				" and `type` = 'insert' ");
		tableEnv.createTemporaryView("test_exam_question", testExamQuestion);

		// TODO 4.从Hbase中查询维度数据 dim_test_question_info
		// 省略 无意义
//		tableEnv.executeSql(HbaseUtil.getCourseInfoLookUpDDL())

		// TODO 5.将数据写入kafka主题中
		// 创建动态表
		tableEnv.executeSql("create table dwd_exam_exam_paper_question(" +
				" id String ," +
				" exam_id String ," +
				" paper_id String ," +
				" question_id String ," +
				" user_id String ," +
				" is_correct String ," +
				" primary key (id) not enforced" +
				")" + KafkaUtil.getUpsertKafkaDDL("dwd_exam_exam_paper_question"));
		// 写入动态表中
		tableEnv.executeSql("insert into dwd_exam_exam_paper_question select * from test_exam_question");

	}

}
