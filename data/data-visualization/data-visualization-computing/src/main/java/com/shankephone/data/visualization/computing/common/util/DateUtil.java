package com.shankephone.data.visualization.computing.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	
	public static String formatDate(Date date, String fmt){
		SimpleDateFormat sdf = new SimpleDateFormat(fmt);
		return sdf.format(date);
	}
	
	public static Date parse(String str, String fmt){
		SimpleDateFormat sdf = new SimpleDateFormat(fmt);
		try {
			return sdf.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

}
