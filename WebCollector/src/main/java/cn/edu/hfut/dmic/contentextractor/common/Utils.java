package cn.edu.hfut.dmic.contentextractor.common;

public class Utils {

	// 1 剔除掉特殊字符" " 编码\\ua0 非正常中文空格
	// 2 剔除掉特殊字符" " 编码\\u3000 非正常中文空格
	private static String ua0 = " ";
	private static String u3000 = "　";

	/**
	 * 加字符串中的非法空格进行替换成标准空格
	 * 
	 * @param src
	 * @return
	 */
	public static String replaceWithSpace(String src) {
		String new_str = src.replaceAll(ua0, " ").replaceAll(u3000, " ");
		return new_str;
	}
}
