package cn.edu.hfut.dmic.contentextractor.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class DT {

	private static long nd = 1000 * 24 * 60 * 60;// 一天的毫秒数
	private static long nh = 1000 * 60 * 60;// 一小时的毫秒数
	private static long nm = 1000 * 60;// 一分钟的毫秒数
	private static long ns = 1000;// 一秒钟的毫秒数
	public static int dayhours = 24;

	/**
	 * 查询开始startTime和endTime之间相差的时分秒数
	 * 
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static Map<String, Long> diffDHMS(Date startTime, Date endTime) {
		long diff;
		long day = 0;
		long hour = 0;
		long min = 0;
		long sec = 0;
		diff = endTime.getTime() - startTime.getTime();
		day = diff / nd;// 计算差多少天
		hour = diff % nd / nh + day * 24;// 计算差多少小时
		min = diff % nd % nh / nm + day * 24 * 60;// 计算差多少分钟
		sec = diff % nd % nh % nm / ns;// 计算差多少秒
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("d", day);
		map.put("h", (hour - day * 24));
		map.put("m", (min - day * 24 * 60));
		map.put("s", sec);
		return map;
	}

	/**
	 * date2比date1多的天数
	 * 
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static int diffDAYS(Date date1, Date date2) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(date1);

		Calendar cal2 = Calendar.getInstance();
		cal2.setTime(date2);
		int day1 = cal1.get(Calendar.DAY_OF_YEAR);
		int day2 = cal2.get(Calendar.DAY_OF_YEAR);

		int year1 = cal1.get(Calendar.YEAR);
		int year2 = cal2.get(Calendar.YEAR);
		if (year1 != year2) {
			int timeDistance = 0;
			for (int i = year1; i < year2; i++) {
				if (i % 4 == 0 && i % 100 != 0 || i % 400 == 0) {
					timeDistance += 366;
				} else {
					timeDistance += 365;
				}
			}
			return timeDistance + (day2 - day1);
		} else {
			System.out.println("判断day2 - day1 : " + (day2 - day1));
			return day2 - day1;
		}
	}

	/**
	 * 采用Java Calendar 设置日期/时间
	 * 
	 * @param year
	 * @param month
	 * @param day
	 * @param hour
	 * @param minute
	 * @param second
	 * @param millisecond
	 * @return
	 */
	public static Date date(int year, int month, int day, int hour, int minute, int second, int millisecond) {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month);
		c.set(Calendar.DAY_OF_MONTH, day);
		c.set(Calendar.HOUR_OF_DAY, hour);
		c.set(Calendar.MINUTE, minute);
		c.set(Calendar.SECOND, second);
		c.set(Calendar.MILLISECOND, millisecond);
		return c.getTime();
	}

	/**
	 * 采用Java Calendar 设置日期/时间
	 * 
	 * @param year
	 * @param month
	 * @param day
	 * @return
	 */
	public static Date date(int year, int month, int day) {
		return date(year, month, day, 0, 0, 0, 0);
	}

	/**
	 * 采用Java Calendar 设置日期/时间
	 * 
	 * @param year
	 * @param month
	 * @param day
	 * @param hour
	 * @param minute
	 * @param second
	 * @return
	 */
	public static Date date(int year, int month, int day, int hour, int minute, int second) {
		return date(year, month, day, hour, minute, second, 0);
	}

	/**
	 * 采用java的SimpleDateFormat将日期转换为字符串
	 * 
	 * @param d
	 * @param formatStr
	 * @return
	 */
	public static String toStr(Date d, String formatStr) {
		if (null == d) {
			return "";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
		return sdf.format(d);
	}

	/**
	 * 将类似“Tue, 12 Jul 2016 02:00:51 GMT”时间转化为本地时间
	 * 
	 * @param gmtStr
	 * @return
	 * @throws ParseException
	 */
	public static Date parseGMT(String gmtStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.US);
		return sdf.parse(gmtStr);
	}

	public static Date parse(String dataStr, String format) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.parse(dataStr);
	}

	public static String toUTCStr(Date date) {
		if (date == null) {
			return "";
		}
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		String timeStr = format.format(date);
		return timeStr;
	}

	public static String getDateFiled(Calendar c, int field) {
		int num = c.get(field);
		if (Calendar.MONTH == field) {
			num++;
		}
		if (num < 10) {
			return "0" + num;
		}
		return num + "";

	}
}