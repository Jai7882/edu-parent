package com.jia.edu.realtime.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;

/**
 * ClassName: KafkaUtil
 * Package: com.jia.edu.realtime.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 14:54
 * @Version 1.0
 */
public class KafkaUtil {

	private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

	public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
		return KafkaSource.<String>builder()
				.setBootstrapServers(KAFKA_SERVER)
				.setTopics(topic)
				.setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new DeserializationSchema<String>() {
					@Override
					public String deserialize(byte[] message) throws IOException {
						if (message != null) {
							return new String(message);
						}
						return null;
					}

					@Override
					public boolean isEndOfStream(String nextElement) {
						return false;
					}

					@Override
					public TypeInformation<String> getProducedType() {
						return Types.STRING;
					}
				})
				.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
				.build();
	}

	public static KafkaSink<String> getKafkaSink(String topic) {
		return KafkaSink.<String>builder()
				.setBootstrapServers(KAFKA_SERVER)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(topic)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build())
				.build();
	}

	public static String getTopiDbDDL(String groupId) {
		return "CREATE TABLE topic_db (\n" +
				"  `database` String,\n" +
				"  `table` String,\n" +
				"  `type` STRING,\n" +
				"  `ts` String ," +
				"  `data` map<String,String> ," +
				"  `old` map<String,String> , " +
				"  `proc_time` as proctime() \n" +
				")" + getKafkaDDL("topic_db1", groupId);
	}

	public static String getKafkaDDL(String topic, String groupId) {
		return "WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = '" + topic + "',\n" +
				"  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
				"  'properties.group.id' = '" + groupId + "',\n" +
				"  'scan.startup.mode' = 'latest-offset',\n" +
				"  'format' = 'json'\n" +
				")";
	}

	// 获得upsertkafka DDL
	public static String getUpsertKafkaDDL(String topic) {
		return  "WITH (\n" +
				"  'connector' = 'upsert-kafka',\n" +
				"  'topic' = '" + topic + "',\n" +
				"  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
				"  'key.format' = 'json' ,\n" +
				"  'value.format' = 'json'\n" +
				")";

	}
}
