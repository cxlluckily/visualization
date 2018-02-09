package com.shankephone.data.visualization.computing.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
	
	/**
	 * 是否含有非数字
	 * @param str
	 * @return
	 */
	public static boolean onlyDigital(String str){
		Pattern p = Pattern.compile("\\D");
		Matcher m = p.matcher(str);
		return !m.find();
	}

}
