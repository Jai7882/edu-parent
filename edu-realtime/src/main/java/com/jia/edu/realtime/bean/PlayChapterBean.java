package com.jia.edu.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: PlayChapterBean
 * Package: com.jia.edu.realtime.bean
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 16:48
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlayChapterBean {

	// 窗口起始时间
	private String stt;
	// 窗口结束时间
	private String edt;
	// 章节id
	private String chapter_id;
	// 章节名称
	private String chapter_name;
	// 统计日期
	private String curDate;
	// 播放次数
	private Integer play_count;
	// 累计播放时长
	private Integer play_sec;
	// 观看人数
	private Integer play_user;
	@JSONField(serialize = false) // 这个字段不需要序列化到json字符串中, 可以加这个注解
	private Long ts;

}
