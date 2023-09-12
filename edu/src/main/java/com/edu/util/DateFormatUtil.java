package com.edu.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * ClassName: DateFormatUtil
 * Package: com.jia.edu.realtime.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/6 14:38
 * @Version 1.0
 */
public class DateFormatUtil {

	public static Integer now(){
		String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
		return Integer.valueOf(yyyyMMdd);
	}
}
