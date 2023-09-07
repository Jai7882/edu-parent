package com.jia.edu.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.bean.TableProcess;
import com.jia.edu.realtime.common.EduConfig;
import com.jia.edu.realtime.util.HbaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * ClassName: DimSinkFunction
 * Package: com.jia.edu.realtime.app.function
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 18:20
 * @Version 1.0
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

	private Connection conn;

	@Override
	public void open(Configuration parameters) throws Exception {
		conn = HbaseUtil.getConnection();
	}

	@Override
	public void close() throws Exception {
		if (conn != null && !conn.isClosed()) {
			conn.close();
		}
	}

	@Override
	public void invoke(JSONObject value, Context context) throws Exception {
		TableProcess tableProcess = value.getObject("table_process", TableProcess.class);
		String type = value.getString("type");
		// 如果mysql 中delete了维度,则hbase 中的维度也应该删除
		if ("delete".equals(type)) {
			System.out.println("从hbase维度表中删除数据");
			String rowKey = value.getString(tableProcess.getSinkRowKey());
			HbaseUtil.delRow(conn, EduConfig.HBASE_NAMESPACE, tableProcess.getSinkTable(), rowKey);
		} else {
			// 其他情况就是put
			System.out.println("往hbase维度表中put数据");
			String rowKey = value.getString(tableProcess.getSinkRowKey());
			String[] columns = tableProcess.getSinkColumns().split(",");
			String[] values = new String[columns.length];
			for (int i = 0; i < values.length; i++) {
				values[i] = value.getString(columns[i]);
			}
			HbaseUtil.putRow(conn,EduConfig.HBASE_NAMESPACE,tableProcess.getSinkTable(),tableProcess.getSinkFamily(),columns,values,rowKey);
		}
	}
}
