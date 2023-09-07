package com.jia.edu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.app.function.DimProcessFunction;
import com.jia.edu.realtime.app.function.DimSinkFunction;
import com.jia.edu.realtime.bean.TableProcess;
import com.jia.edu.realtime.common.EduConfig;
import com.jia.edu.realtime.util.HbaseUtil;
import com.jia.edu.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Properties;

/**
 * ClassName: DimApp
 * Package: com.jia.edu.realtime.app.dim
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 14:43
 * @Version 1.0
 */
public class DimApp {

	public static void main(String[] args) {
		// TODO 1.获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度 和kafka分区数匹配
		env.setParallelism(4);

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

		// TODO 3.从Kafka的topic_db主题中获取主流数据
		String topic = "topic_db1";
		String groupId = "dim_app_group";
		KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
		DataStreamSource<String> topicDbDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
		// 将数据转换为JsonObject对象方便后续处理
		SingleOutputStreamOperator<JSONObject> processedDs = topicDbDs.process(new ProcessFunction<String, JSONObject>() {
			@Override
			public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
				JSONObject jsonObject = JSON.parseObject(value);
				// 数据清洗 将 bootstrap-start和 bootstrap-complete 过滤
				String type = jsonObject.getString("type");
				if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
					out.collect(jsonObject);
				}
			}
		});

		// TODO 4.通过FlinkCDC读取Dim对应配置文件
		// 获取mysql中对应配置库中的配置表 通过flinkCDC的方式实时捕获dim表的变化
		// 读取配置表
		Properties properties = new Properties();
		properties.setProperty("useSSL", "false");
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
				.username("root")
				.password("000000")
				.hostname("hadoop102")
				.port(3306)
				.jdbcProperties(properties)
				.databaseList("edu_config")
				.tableList("edu_config.table_process_dim")
				.startupOptions(StartupOptions.initial())
				.deserializer(new JsonDebeziumDeserializationSchema())
				.build();

		DataStreamSource<String> mysqlDs = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");
		// 将配置流进一步进行处理 通过op字段封装为不同的bean对象 最后返回
		// 当操作类型为d delete时 数据中应包含before字段 表示删除之前的数据
		// 其他类型 cru create read update 时 数据应包含after字段表示配置数据修改之后的状态
		SingleOutputStreamOperator<TableProcess> tableProcessed = mysqlDs.process(
				new ProcessFunction<String, TableProcess>() {
					@Override
					public void processElement(String value, ProcessFunction<String, TableProcess>.Context ctx, Collector<TableProcess> out) throws Exception {
						// 先将数据转为JsonObject对象方便后续操作
						JSONObject jsonObject = JSON.parseObject(value);
						String op = jsonObject.getString("op");
						TableProcess tableProcess = null;
						if ("d".equals(op)) {
							tableProcess = jsonObject.getObject("before", TableProcess.class);
						} else {
							tableProcess = jsonObject.getObject("after", TableProcess.class);
						}
						tableProcess.setOp(op);
						out.collect(tableProcess);
					}
				}
		);

		// TODO 6.在hbase中根据对象中op的值对应的在hbase中操作
		// 当 op为c create r read 时 对应着需要在hbase中创建对应的维度表
		// d delete 删除 u update 先删除 后创建
		tableProcessed = tableProcessed.map(
				// 由于需要操作hbase数据库 所以需要hbase的连接 创建连接属于重量级操作 不应每次操作都创建一次连接
				// 故通过富函数中的 open 和 close 方法对连接进行初始化获取和关闭操作
				new RichMapFunction<TableProcess, TableProcess>() {
					private Connection connection = null;

					@Override
					public void open(Configuration parameters) throws Exception {
						connection = HbaseUtil.getConnection();
					}

					@Override
					public void close() throws Exception {
						HbaseUtil.closeConnection(connection);
					}

					@Override
					public TableProcess map(TableProcess value) throws Exception {
						String op = value.getOp();
						String sinkTable = value.getSinkTable();
						String sinkFamily = value.getSinkFamily();
						if ("d".equals(op)) {
							// delete 直接drop
							HbaseUtil.dropTable(connection, EduConfig.HBASE_NAMESPACE, sinkTable);
						} else if ("c".equals(op) || "r".equals(op)) {
							// create read 直接create
							HbaseUtil.createTable(connection, EduConfig.HBASE_NAMESPACE, sinkTable, sinkFamily.split(","));
						} else {
							// update 先drop 再 create
							HbaseUtil.dropTable(connection, EduConfig.HBASE_NAMESPACE, sinkTable);
							HbaseUtil.createTable(connection, EduConfig.HBASE_NAMESPACE, sinkTable, sinkFamily.split(","));
						}
						return value;
					}
				}
		);

		// TODO 7.广播配置流
		MapStateDescriptor<String, TableProcess> state = new MapStateDescriptor<String, TableProcess>("state", String.class, TableProcess.class);
		BroadcastStream<TableProcess> broadcastDs = tableProcessed.broadcast(state);

		// TODO 8.主流连接广播流配置
		// 通过自定义处理广播流函数处理
		BroadcastConnectedStream<JSONObject, TableProcess> connected = processedDs.connect(broadcastDs);
		SingleOutputStreamOperator<JSONObject> processed = connected.process(new DimProcessFunction(state));

		// TODO 9.将数据写入到Hbase的维度表中
		processed.addSink(new DimSinkFunction());

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
