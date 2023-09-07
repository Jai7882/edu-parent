package com.jia.edu.realtime.util;

import com.jia.edu.realtime.common.EduConfig;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;

/**
 * ClassName: DorisUtil
 * Package: com.jia.edu.realtime.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/7 22:36
 * @Version 1.0
 */
public class DorisUtil {

	public static DorisSink<String> getDorisSink(String tableName) {
		Properties properties = new Properties();
		properties.setProperty("format", "json");
		properties.setProperty("read_json_by_line", "true");
		return DorisSink.<String>builder()
				.setDorisOptions(DorisOptions.builder()
						.setFenodes(EduConfig.DORIS_FE)
						.setUsername("root")
						.setPassword("000000")
						.setTableIdentifier(EduConfig.DORIS_DB + "." + tableName)
						.build())
				.setDorisExecutionOptions(DorisExecutionOptions.builder()
						.disable2PC()
						.setMaxRetries(3)
						.setBufferSize(8*1024)
						.setBufferCount(3)
						.setStreamLoadProp(properties)
						.setDeletable(false)
						.build())
				.setSerializer(new SimpleStringSerializer())
				.build();
	}

}
