package com.jia.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TableProcess
 * Package: com.jia.edu.realtime.bean
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 15:46
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {
	// 来源表
	private String sourceTable;
	// 输出表
	private String sinkTable;
	// 输出列族
	private String sinkFamily;
	// 输出字段
	private String sinkColumns;
	// 输出主键
	private String sinkRowKey;
	// 配置表操作: c r u d
	private String op;

}
