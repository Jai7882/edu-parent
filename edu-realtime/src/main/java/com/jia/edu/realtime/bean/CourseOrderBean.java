package com.jia.edu.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * ClassName: CourseOrderBean
 * Package: com.jia.edu.realtime.bean
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/8 9:50
 * @Version 1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CourseOrderBean {
	// 窗口起始时间
	private String stt;
	// 窗口结束时间
	private String edt;
	// 来源
	private String sc;
	// 来源名称
	private String sourceName;
	// 课程id
//	private String courseId ;
//	// 课程名称
//	private String courseName;
//	// 学科id
//	private String subjectId ;
//	// 学科名称
//	private String subjectName;
//	// 类别id
//	private String categoryId ;
//	// 类别名称
//	private String categoryName;
	// 省份id
	private String provinceId;
	// 省份名称
	private String provinceName;
	// 当天日期
	private String curDate;
	// 下单次数
	private Long orderCt;
	// 下单人数
	private Long orderUserCt;
	// 订单金额
	private BigDecimal orderAmount;
	// 时间戳
	@JSONField(serialize = false) // 这个字段不需要序列化到json字符串中, 可以加这个注解
	private Long ts;
}
