package com.jia.edu.realtime.util;

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

	private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public static Long toTs(String dtStr, boolean isFull) {

		LocalDateTime localDateTime = null;
		if (!isFull) {
			dtStr = dtStr + " 00:00:00";
		}
		localDateTime = LocalDateTime.parse(dtStr, dtfFull);

		return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
	}

	public static Long toTs(String dtStr) {
		return toTs(dtStr, false);
	}

	public static String toDate(Long ts) {
		Date dt = new Date(ts);
		LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
		return dtf.format(localDateTime);
	}

	public static String toYmdHms(Long ts) {
		Date dt = new Date(ts);
		LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
		return dtfFull.format(localDateTime);
	}


	public static String toPartitionDate(long start) {
		return null;
	}
}
