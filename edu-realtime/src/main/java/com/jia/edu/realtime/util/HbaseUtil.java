package com.jia.edu.realtime.util;

import com.jia.edu.realtime.common.EduConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ClassName: HbaseUtil
 * Package: com.jia.edu.realtime.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 15:59
 * @Version 1.0
 */
public class HbaseUtil {

	// 获取Hbase连接
	public static Connection getConnection() {
		Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "hadoop102");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			return ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// 关闭Hbase连接
	public static void closeConnection(Connection connection) {
		if (connection != null && !connection.isClosed()) {
			try {
				connection.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	// Hbase创建表
	public static void createTable(Connection connection, String hbaseNamespace, String sinkTable, String... columns) {
		if (columns == null || columns.length < 1) {
			throw new RuntimeException("在建表时必须提供列族");
		}
		try (Admin admin = connection.getAdmin()) {
			// 创建前先判断表是否已存在
			TableName tableName = TableName.valueOf(hbaseNamespace, sinkTable);
			if (admin.tableExists(tableName)) {
				System.out.println("要创建的" + hbaseNamespace + ":" + sinkTable + "表已存在!!!!!!");
				return;
			} else {
				System.out.println("在Hbase中创建" + hbaseNamespace + ":" + sinkTable + "表!!!");
				TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
				for (String column : columns) {
					builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column)).build());
				}
				TableDescriptor descriptor = builder.build();
				admin.createTable(descriptor);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// Hbase删除表
	public static void dropTable(Connection connection, String hbaseNamespace, String sinkTable) {
		try (Admin admin = connection.getAdmin()) {
			TableName tableName = TableName.valueOf(hbaseNamespace, sinkTable);
			if (!admin.tableExists(tableName)) {
				System.out.println("要删除的" + hbaseNamespace + ":" + sinkTable + "表不存在");
				return;
			} else {
				System.out.println("删除Hbase中的" + hbaseNamespace + ":" + sinkTable + "表");
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void delRow(Connection conn, String hbaseNamespace, String sinkTable, String rowKey) {
		// 删除对应rowKey的数据
		try (Table table = conn.getTable(TableName.valueOf(hbaseNamespace, sinkTable))){
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			table.delete(delete);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void putRow(Connection conn, String hbaseNamespace, String sinkTable, String sinkFamily, String[] columns, String[] values, String sinkRowKey) {
		try (Table table = conn.getTable(TableName.valueOf(hbaseNamespace, sinkTable))){
			Put put = new Put(Bytes.toBytes(sinkRowKey));
			for (int i = 0; i < columns.length; i++) {
				String column = columns[i];
				String value = values[i];
				// 有部分列的值是 null, 不用写出
				if (value!=null){
					put.addColumn(Bytes.toBytes(sinkFamily),Bytes.toBytes(column),Bytes.toBytes(value));
				}
			}
			table.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getCourseInfoLookUpDDL() {
		// 原子类型会自动识别为 hbase 的 RowKey,只能有一个原子类型. 列名随意
		return "CREATE TABLE course_info (" +
				" id string, " +
				" info ROW<course_name String> ," +
				" PRIMARY KEY (id) NOT ENFORCED )" +
				getHbaseDDL(EduConfig.HBASE_NAMESPACE +":dim_course_info");
	}

	private static String getHbaseDDL(String tableName) {
		return " with(" +
				" 'connector' = 'hbase-2.2'," +
				" 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104', " +
				" 'table-name' = '" + tableName + "', " +
				" 'lookup.async' = 'true', " +
				" 'lookup.cache' = 'PARTIAL'," +
				" 'lookup.partial-cache.max-rows' = '200'," +
				" 'lookup.partial-cache.expire-after-write' = '1 hour', " +
				" 'lookup.partial-cache.expire-after-access' = '1 hour'" +
				")";
	}

	public static String getTestPaperLookUpDDL() {
		return "CREATE TABLE test_paper (" +
				" id string, " +
				" info ROW<course_id String> ," +
				" PRIMARY KEY (id) NOT ENFORCED )" +
				getHbaseDDL(EduConfig.HBASE_NAMESPACE +":dim_test_paper");

	}
}
