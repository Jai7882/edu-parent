package com.jia.edu.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * ClassName: DimProcessFunction
 * Package: com.jia.edu.realtime.app.function
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 16:34
 * @Version 1.0
 */
public class DimProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcess, JSONObject> {

	private MapStateDescriptor<String, TableProcess> state;
	// 预加载配置
	private HashMap<String, TableProcess> configMap = new HashMap<>();

	// 预加载配置
	@Override
	public void open(Configuration parameters) throws Exception {
		Class.forName("com.mysql.cj.jdbc.Driver");
		Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?user=root&password=000000");
		PreparedStatement ps = connection.prepareStatement("select * from edu_config.table_process_dim");
		ResultSet resultSet = ps.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();
		while (resultSet.next()) {
			// 定义Json对象封装查询出的结果
			JSONObject jsonObject = new JSONObject();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				String columnName = metaData.getColumnName(i);
				Object object = resultSet.getObject(i);
				jsonObject.put(columnName, object);
			}
			// 将JsonObject对象转换为实体类对象
			TableProcess tableProcess = jsonObject.toJavaObject(TableProcess.class);
			// 将读取到的配置信息添加到map集合中
			configMap.put(tableProcess.getSourceTable(), tableProcess);
		}

		// 资源释放
		resultSet.close();
		ps.close();
		connection.close();
	}

	public DimProcessFunction(MapStateDescriptor<String, TableProcess> state) {
		this.state = state;
	}

	@Override
	public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
		//{"database":"gmall","xid":203730,"data":{"tm_name":"华为221","create_time":"2021-12-14 00:00:00","logo_url":"/static/default.jpg","id":3},
		//	"old":{"tm_name":"华为22"},"commit":true,"	type":"update","table":"base_trademark","ts":1692630820}
		ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(state);
		String table = value.getString("table");
		TableProcess tableProcess = null;
		if ((tableProcess = broadcastState.get(table)) != null ||
				(tableProcess = configMap.get(table)) != null){
			// 说明状态中有值 说明是维度数据 将维度数据获得 并过滤掉不需要的字段 最后将type 和 主流数据put到JsonObject中
			JSONObject object = value.getJSONObject("data");
			String type = value.getString("type");
			filterColumn(object,tableProcess.getSinkColumns());
			object.put("type",type);
			object.put("table_process",tableProcess);
			out.collect(object);
		}
	}

	private void filterColumn(JSONObject object, String sinkColumns) {
		String[] columns = sinkColumns.split(",");
		List<String> list = Arrays.asList(columns);
		Set<Map.Entry<String, Object>> entries = object.entrySet();
		entries.removeIf(e->!list.contains(e.getKey()));
	}

	@Override
	public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
		BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(state);
		String sourceTable = value.getSourceTable();
		String op = value.getOp();
		if ("d".equals(op)) {
			broadcastState.remove(sourceTable);
			configMap.remove(sourceTable);
		} else {
			broadcastState.put(sourceTable, value);
			configMap.put(sourceTable, value);
		}
	}
}
