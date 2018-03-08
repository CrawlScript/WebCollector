package cn.edu.hfut.dmic.contentextractor;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.hfut.dmic.contentextractor.common.DT;
import cn.edu.hfut.dmic.contentextractor.common.Utils;

/**
 * 发布时间提取类
 * @author imevis
 *
 */
public class PublishTimeExtractor {
	public static Logger logger = LoggerFactory.getLogger(PublishTimeExtractor.class);
	//public static String FULL_REGEX = "(19[8-9]{1}\\d{1}|2\\d{3})[^\\d]{1,5}?(0?[1-9]{1}|1[1-2]{1})[^\\d]{1,5}?(0?\\d{1}|[1-2]{1}\\d{1}|[3]{1}[0-1]{1})[^\\d]{1,5}?([0-1]?\\d{1}|2[0-3]{1})?[^\\d]{1,5}?([0-5]?\\d{1})?[^\\d]{1,5}?([0-5]?\\d{1})?";

	/** 带日期和时间正则 */
	public static String LONG_DATE_TIME_REGEX = "([0-9]{2,4}[\\s]*[/|\\-|年|\\.]){1}[\\s]*[0-9]{1,2}[\\s]*[/|\\-|月|\\.]+[\\s]*[0-9]{1,2}[日|\\s]*([0-9]{1,2}[:|时])*([0-9]{1,2}[:|分])*([0-9]{1,2}[秒]?)*";

	/** 不带年的时间正则 */
	public static String SHORT_DATE_TIME_REGEX = "[0-9]{1,2}[/|\\-|月]+[0-9]{1,2}[日|\\s]+([0-9]{1,2}[:|时])*([0-9]{1,2})*([:|分])?([0-9]{1,2}[秒]?)?";

	/** 带日期不带时间正则 */
	public static String LONG_DATE_REGEX = "[0-9]{2,4}[\\s]*[/|\\-|年|\\s|\\.]?[\\s]*[0-9]{1,2}[\\s]*[/|\\-|月|\\s|\\.]+[\\s]*[0-9]{1,2}日?";

	/** 纯数字正则 */
	public static String NUM_ONLY_TIME_REGEX = "[0-9]{2,4}[0-9]{2}[0-9]{2}";

	public static String LONG_DATE_TIME_REGEX_M = ".*" + LONG_DATE_TIME_REGEX + ".*";

	public static String LONG_DATE_REGEX_M = ".*" + LONG_DATE_REGEX + ".*";

	public static String SHORT_DATE_TIME_REGEX_M = ".*" + SHORT_DATE_TIME_REGEX + ".*";

	public static String NUM_ONLY_TIME_REGEX_M = ".*" + NUM_ONLY_TIME_REGEX + ".*";

	/**
	 * 从字符串提取, 提取失败就返回空
	 * 
	 * @param str
	 * @return
	 */
	public static Date extractDate(String str) {
		try {
			if (!StringUtils.isEmpty(str)) {
				return parseDate(str);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 解析字符串中出现的日期
	 * 
	 * @param targetStr
	 * @return
	 */
	public static String fetchDateTime(String targetStr) {
		if (StringUtils.isEmpty(targetStr)) {
			return null;
		}
		String cvtStr = targetStr;
		cvtStr = Utils.replaceWithSpace(cvtStr);

		// 如果是带年和日期格式
		String dateStr = null;
		if (cvtStr.matches(LONG_DATE_TIME_REGEX_M)) {
			Pattern datetime_pattern = Pattern.compile(LONG_DATE_TIME_REGEX);
			Matcher matcher = datetime_pattern.matcher(cvtStr);
			if (matcher.find()) {
				dateStr = matcher.group(0);
				return replacePh(dateStr);
			}
		}
		// 如果是不带年
		if (cvtStr.matches(SHORT_DATE_TIME_REGEX_M)) {
			Pattern datetime_pattern = Pattern.compile(SHORT_DATE_TIME_REGEX);
			Matcher matcher = datetime_pattern.matcher(cvtStr);
			if (matcher.find()) {
				dateStr = matcher.group(0);
				// 将年份加上
				dateStr = DT.toStr(new Date(), "yyyy-") + dateStr;
				return replacePh(dateStr);
			}
		}

		if (cvtStr.matches(LONG_DATE_REGEX_M)) {
			Pattern datetime_pattern = Pattern.compile(LONG_DATE_REGEX);
			Matcher matcher = datetime_pattern.matcher(cvtStr);
			if (matcher.find()) {
				dateStr = matcher.group(0);
				return replacePh(dateStr);
			}
		}

		// 判断是否用日 milliseconds 表示的日期long型字符串表示的日期
		if (cvtStr.matches(".*[\\d]{13}.*")) {
			Pattern datetime_pattern = Pattern.compile("[\\d]{13}");
			Matcher matcher = datetime_pattern.matcher(cvtStr);
			if (matcher.find()) {
				String milliseconds_str = matcher.group(0);
				try {
					long milliseconds = Long.valueOf(milliseconds_str);
					Date date = new Date(milliseconds);
					return DT.toStr(date, "yyyy-MM-dd HH:mm:ss");
				} catch (Throwable e) {
					return null;
				}
			}
		}

		if (cvtStr.matches("[\\s]*今天.*")) {
			cvtStr = cvtStr.replaceAll("[\\s]*今天", DT.toStr(new Date(), "yyyy-MM-dd "));
			return cvtStr;
		}

		// 处理 *分钟前
		if (cvtStr.matches(".*[0-9]{1,2}分钟前.*")) {
			Pattern datetime_pattern = Pattern.compile("[0-9]{1,2}分钟前");
			Matcher matcher = datetime_pattern.matcher(cvtStr);
			if (matcher.find()) {
				String minutesAgo = matcher.group(0);
				minutesAgo = minutesAgo.replace("分钟前", "");
				Integer mago = null;
				try {
					mago = Integer.valueOf(minutesAgo);
				} catch (Throwable e) {
					return null;
				}
				if (mago == null)
					return null;
				Calendar cl = Calendar.getInstance();
				cl.setTime(new Date());
				cl.add(Calendar.MINUTE, (mago * -1));
				return DT.toStr(cl.getTime(), "yyyy-MM-dd HH:mm:ss");
			}
		}

		// 都不满足，用纯数字日期提取尝试
		if (cvtStr.matches(NUM_ONLY_TIME_REGEX_M)) {
			Pattern datetime_pattern = Pattern.compile(NUM_ONLY_TIME_REGEX);
			Matcher matcher = datetime_pattern.matcher(cvtStr);
			if (matcher.find()) {
				dateStr = matcher.group(0);
				Date d;
				try {
					d = DT.parse(dateStr, "yyyyMMdd");
					return DT.toStr(d, "yyyy-MM-dd");
				} catch (ParseException e) {
					return null;
				}
			}
		}

		return dateStr;
	}

	/**
	 * 进行关键字替换 如 年 月 日 - 等字符
	 * 
	 * @param dateStr
	 * @return
	 */
	public static String replacePh(String dateStr) {
		if (StringUtils.isEmpty(dateStr)) {
			return null;
		}
		// 将年月日替换为"-"
		String newStr = dateStr.replaceAll("\\s*年\\s*", "-").replaceAll("\\s*月\\s*", "-").replaceFirst("日\\s*", " ")
				.replaceFirst("时\\s*", ":").replaceFirst("分\\s*", ":").replaceAll("/", "-").replaceAll("\\.", "-");
		return newStr;
	}

	/**
	 * 解析日期格式字符串
	 * 
	 * @param dataStr
	 * @param format
	 * @return
	 */
	public static Date parseDate(String dataStr, String format) {
		if (StringUtils.isEmpty(dataStr)) {
			return null;
		}
		try {
			return DT.parse(dataStr, format);
		} catch (ParseException e) {
			return null;
		}
	}

	/**
	 * 解析日期格式字符串
	 * 
	 * @param dataStr
	 * @param format
	 * @return
	 */
	public static Date parseDate(String dateStr) {
		dateStr = fetchDateTime(dateStr);
		return parseDate(dateStr, dateStr2Format(dateStr));
	}

	/**
	 * 时间字符串转换格式化格式
	 * 
	 * @param dateStr
	 * @return
	 */
	private static String dateStr2Format(String dateStr) {
		if (StringUtils.isEmpty(dateStr)) {
			return null;
		}
		String formatYear = "yyyy";
		if (dateStr.matches("[0-9]{2}-[0-9]{1,2}-[0-9]{1,2}.*")) {// 2位年份
			formatYear = "yy";
		} else {// 4位年份
			formatYear = "yyyy";
		}
		String format = "-MM-dd HH:mm:ss";
		if (StringUtils.countMatches(dateStr, ":") == 1) {// 不带秒
			format = "-MM-dd HH:mm";
		} else if (StringUtils.countMatches(dateStr, ":") == 0) {// 不带时间
			format = "-MM-dd";
		} else {
			format = "-MM-dd HH:mm:ss";
		}
		return format = formatYear + format;
	}
}
