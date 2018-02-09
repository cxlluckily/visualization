package com.shankephone.data.visualization.computing.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.ClassUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;

/**
 * 统计分析通用处理接口
 * @author fengql
 * @version 2017年7月24日 下午5:46:23
 */
public interface IAnalyseHandler extends ICommonHandler{
	
	public final static Logger logger = LoggerFactory.getLogger(IAnalyseHandler.class);

	/**
	 * 日历设置
	 * @author fengql
	 * @date 2017年7月21日 下午2:15:34
	 * @return
	 */
	public static Calendar getCalendarInstance(){
		Calendar cal = Calendar.getInstance();
		cal.setFirstDayOfWeek(Calendar.MONDAY);
		cal.setMinimalDaysInFirstWeek(Constants.FIRST_WEEK_MIN_DAYS);
		return cal;
	}
	
	/**
	 * 获取对应的时段值
	 * @author fengql
	 * @date 2017年7月21日 下午2:15:41
	 * @param periodType
	 * @param starttime
	 * @return
	 */
	public static String getTimePeriod(Integer periodType, String starttime){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date start = null;
		Calendar cal = getCalendarInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 小时
		if (periodType.intValue() == Constants.PERIOD_TYPE_HOUR.intValue()) {
			try {
				start = format.parse(starttime);
			} catch (Exception e) {
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			return format.format(cal);
		}
		//天，取日期部分
		if(periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()){
			if(starttime != null && !"".equals(starttime)){
				return null;
			}
			return starttime.split(" ")[0];
		}
		// 周,周的格式为 【年-周】，如yyyy-ww
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				e.printStackTrace();
			}
			cal.setTime(start);
			int year = cal.get(Calendar.YEAR);
			int week = cal.get(Calendar.WEEK_OF_YEAR);
			int month = cal.get(Calendar.MONTH);
			if(month > 10 && week < 2){
				year += 1;
				//System.out.println(starttime + ":" + year + "-" + week);
			}
			if(month < 2 && week > 50){
				year -= 1;
				//System.out.println(starttime + ":" + year + "-" + week);
			}
			String result = year + "-" + week;
			return result;
		}
		// 月
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				e.printStackTrace();
			}
			cal.setTime(start);
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			return year + "-" + month;
		}
		// 年
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				e.printStackTrace();
			}

			cal.setTime(start);
			int year = cal.get(Calendar.YEAR);
			return year + "";
		}
		return null;
	}
	
	/**
	 * 获取对应的时段值
	 * @author fengql
	 * @date 2017年7月21日 下午2:15:41
	 * @param periodType
	 * @param starttime
	 * @return
	 */
	public static String getDatePeriod(Integer periodType, String starttime){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date start = null;
		Calendar cal = getCalendarInstance();
		//天，取日期部分
		if(periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()){
			if(starttime != null && !"".equals(starttime)){
				return null;
			}
			return starttime.split(" ")[0];
		}
		// 周,周的格式为 【年-周】，如yyyy-ww
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				e.printStackTrace();
			}
			cal.setTime(start);
			int year = cal.get(Calendar.YEAR);
			int week = cal.get(Calendar.WEEK_OF_YEAR);
			int month = cal.get(Calendar.MONTH);
			if(month > 10 && week < 2){
				year += 1;
				//System.out.println(starttime + ":" + year + "-" + week);
			}
			if(month < 2 && week > 50){
				year -= 1;
				//System.out.println(starttime + ":" + year + "-" + week);
			}
			String result = year + "-" + week;
			return result;
		}
		// 月
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				e.printStackTrace();
			}
			cal.setTime(start);
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			return year + "-" + month;
		}
		// 年
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				e.printStackTrace();
			}

			cal.setTime(start);
			int year = cal.get(Calendar.YEAR);
			return year + "";
		}
		return null;
	}
	
	/**
	 * 根据时段类型获取查询与分组字段
	 * @author fengql
	 * @date 2017年6月21日 上午9:14:43
	 * @param periodType
	 * @return
	 */
	public static String[] getPeriodFields(Integer periodType) {
		String[] fields = new String[2];
		// 按小时算
		if (periodType.intValue() == Constants.PERIOD_TYPE_HOUR.intValue()) {
			fields[0] = " HOURS ";
			fields[1] = " HOURS asc";
		}
		// 按天算
		if (periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()) {
			fields[0] = " DAYS ";
			fields[1] = " DAYS asc";
		}
		// 按周算
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			fields[0] = " WEEKS ";
			fields[1] = " WEEKS asc";
		}
		// 按月算
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			fields[0] = " MONTHS ";
			fields[1] = " MONTHS asc";
		}
		// 按年算
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			fields[0] = " YEARS ";
			fields[1] = " YEARS asc";
		}
		return fields;
	}
	
	/**
	 * 获取开始时间，根据时间段类型和开始时间
	 * @author fengql
	 * @date 2017年6月21日 下午3:24:04
	 * @param periodType
	 * @param starttime
	 * @return
	 */
	public static String getStartTime(Integer periodType, String starttime){
		Date start = null;
		Calendar cal = IAnalyseHandler.getCalendarInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 小时
		if (periodType.intValue() == Constants.PERIOD_TYPE_HOUR.intValue()) {
			try {
				start = format.parse(starttime);
			} catch (Exception e) {
				logger.error("获取开始时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
		}
		// 天
		if (periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.error("获取开始时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.HOUR_OF_DAY, 0);
		}
		// 周,周的格式为 【年-周】，如yyyy-ww
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			String starts [] = starttime.split("-");
			String years = starts[0];
			String weeks = starts[1];
			int year = Integer.parseInt(years);
			int week = Integer.parseInt(weeks);
			
			//设置时间为当前周
			cal.set(Calendar.YEAR, year);
			cal.set(Calendar.WEEK_OF_YEAR, week);
			//获取并定位到当前周的第一天位置
			int first = cal.getFirstDayOfWeek();		
			cal.set(Calendar.DAY_OF_WEEK, first);
			cal.set(Calendar.HOUR_OF_DAY, 0);
		}
		// 月
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.error("获取开始时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
		}
		// 年
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.error("获取开始时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}

			cal.setTime(start);
			cal.set(Calendar.MONTH, 0);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
		}
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		String time = format.format(cal.getTime());
		return time;
	}
	
	/**
	 * 获取结束时间，根据时间段类型和开始时间
	 * @author fengql
	 * @date 2017年6月21日 下午3:23:29
	 * @param periodType
	 * @param starttime
	 * @return
	 */
	public static String getEndTime(Integer periodType, String starttime) {
		Date start = null;
		String endtime = "";
		Calendar cal = IAnalyseHandler.getCalendarInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 小时
		if (periodType.intValue() == Constants.PERIOD_TYPE_HOUR.intValue()) {
			try {
				start = format.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
		}
		// 天
		if (periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.HOUR_OF_DAY, 23);
		}
		// 周
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			String starts [] = starttime.split("-");
			String years = starts[0];
			String weeks = starts[1];
			int year = Integer.parseInt(years);
			int week = Integer.parseInt(weeks);
			
			//设置时间为当前周
			cal.set(Calendar.YEAR, year);
			cal.set(Calendar.WEEK_OF_YEAR, week);
			//获取当前周的第一天位置
			int first = cal.getFirstDayOfWeek();		
			cal.set(Calendar.DAY_OF_WEEK, first);
			//System.out.println("周开始日期：" + format.format(cal.getTime()));
			//定位到当前周的最后一天
			cal.add(Calendar.DAY_OF_YEAR, 6);
			//System.out.println("结束日期" + format.format(cal.getTime()));
			cal.set(Calendar.HOUR_OF_DAY, 23);
		}
		// 月
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.DATE, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
			cal.set(Calendar.HOUR_OF_DAY, 23);
		}
		// 年
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束时间-错误时间：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.MONTH, 11);
			cal.set(Calendar.DAY_OF_MONTH, 31);
			cal.set(Calendar.HOUR_OF_DAY, 23);
		}
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		endtime = format.format(cal.getTime());
		return endtime;
	}
	
	/**
	 * 获取开始时间，根据时间段类型和开始时间
	 * @author fengql
	 * @date 2017年6月21日 下午3:24:04
	 * @param periodType
	 * @param starttime
	 * @return
	 */
	public static Date getStartDate(Integer periodType, String starttime){
		Date start = null;
		Calendar cal = getCalendarInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 小时
		if (periodType.intValue() == Constants.PERIOD_TYPE_HOUR.intValue()) {
			try {
				start = format.parse(starttime);
			} catch (Exception e) {
				logger.info("获取开始日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			return cal.getTime();
		}
		// 天
		if (periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取开始日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
		}
		// 周,周的格式为 【年-周】，如yyyy-ww
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			String starts [] = starttime.split("-");
			String years = starts[0];
			String weeks = starts[1];
			int year = Integer.parseInt(years);
			int week = Integer.parseInt(weeks);
			
			//设置时间为当前周
			cal.set(Calendar.YEAR, year);
			cal.set(Calendar.WEEK_OF_YEAR, week);
			//获取并定位到当前周的第一天位置
			int first = cal.getFirstDayOfWeek();		
			cal.set(Calendar.DAY_OF_WEEK, first);
		}
		// 月
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取开始日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.DAY_OF_MONTH, 1);
		}
		// 年
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取开始日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}

			cal.setTime(start);
			cal.set(Calendar.MONTH, 0);
			cal.set(Calendar.DAY_OF_MONTH, 1);
		}
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTime();
	}
	
	/**
	 * 获取结束时间，根据时间段类型和开始时间
	 * @author fengql
	 * @date 2017年6月21日 下午3:23:29
	 * @param periodType
	 * @param starttime
	 * @return
	 */
	public static Date getEndDate(Integer periodType, String starttime) {
		Date start = null;
		Calendar cal = getCalendarInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 小时
		if (periodType.intValue() == Constants.PERIOD_TYPE_HOUR.intValue()) {
			try {
				start = format.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.MINUTE, 59);
			cal.set(Calendar.SECOND, 59);
			return cal.getTime();
		}
		// 天
		if (periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
		}
		// 周
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			String starts [] = starttime.split("-");
			String years = starts[0];
			String weeks = starts[1];
			int year = Integer.parseInt(years);
			int week = Integer.parseInt(weeks);
			
			//设置时间为当前周
			cal.set(Calendar.YEAR, year);
			cal.set(Calendar.WEEK_OF_YEAR, week);
			//获取当前周的第一天位置
			int first = cal.getFirstDayOfWeek();		
			cal.set(Calendar.DAY_OF_WEEK, first);
			//System.out.println("周开始日期：" + format.format(cal.getTime()));
			//定位到当前周的最后一天
			cal.add(Calendar.DAY_OF_YEAR, 6);
			//System.out.println("结束日期" + format.format(cal.getTime()));
		}
		// 月
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.DATE, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
		}
		// 年
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
			try {
				start = sdf.parse(starttime);
			} catch (Exception e) {
				logger.info("获取结束日期-错误日期：(" + starttime + ")");
				e.printStackTrace();
			}
			cal.setTime(start);
			cal.set(Calendar.MONTH, 11);
			cal.set(Calendar.DAY_OF_MONTH, 31);
		}
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		return cal.getTime();
	}
	
	/**
	 * 根据时段获取对应查询字段
	 * @author fengql
	 * @date 2017年7月12日 下午6:26:23
	 * @param periodType
	 * @return
	 */
	public static String getPeriodField(Integer periodType) {
		String period = "";
		if (periodType.intValue() == Constants.PERIOD_TYPE_DATE.intValue()) {
			period = "days";
		}
		// 按周算
		if (periodType.intValue() == Constants.PERIOD_TYPE_WEEK.intValue()) {
			period = "weeks";
		}
		// 按月算
		if (periodType.intValue() == Constants.PERIOD_TYPE_MONTH.intValue()) {
			period = "months";
		}
		// 按年算
		if (periodType.intValue() == Constants.PERIOD_TYPE_YEAR.intValue()) {
			period = "years";
		}
		return period;
	}
	
	/**
	 * 统一分析接口方法，用于调用实例的业务方法
	 * @author fengql
	 * @date 2017年8月22日 下午3:52:41
	 */
	public void analyse(String timestamp);
	
	public static String getLongTimestamp(String timestamp){
		if(timestamp == null || "".equals(timestamp)){
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = sdf.parse(timestamp);
			return date.getTime() + "";
		} catch (ParseException e) {
			sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				date = sdf.parse(timestamp);
				return date.getTime() + "";
			} catch (ParseException e1) {
				return timestamp;
			}
		}
	}
	
	public static String getSQL(String sqlFile, Date startDate, Long end){
		String sql = null;
		try {
			sql = IOUtils.toString(ClassUtils.getResourceAsStream("sqls/" + sqlFile), "UTF-8");
		} catch (Exception e) {
			logger.info("error: " + e.getMessage());
		}
		//设置SQL时间条件
		sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_PREFIX, "");
		sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_SUFFIX, "");
		//pay_pay_time数据时间限制
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
		sql = sql.replace("$[1]", "'" + sdf.format(startDate) + "'");
		sql = sql.replace("$[2]", "'" + end + "'");
		return sql;
	}
	
	public static String getLongTimestamp(String timestamp, Integer beforeNums){
		String datetime = getLongTimestamp(timestamp);
		if(datetime == null || "".equals(datetime)){
			Date date = new Date();
			datetime = date.getTime() + "";
		}
		if(beforeNums != null){
			long time = Long.parseLong(datetime);
			Date end = new Date(time);
			Calendar cal = Calendar.getInstance();
			cal.setTime(end);
			cal.add(Calendar.DAY_OF_YEAR, -beforeNums);
			cal.set(Calendar.HOUR_OF_DAY, 23);
			cal.set(Calendar.MINUTE, 59);
			cal.set(Calendar.SECOND, 59);
			time = cal.getTimeInMillis();
			datetime = time + "";
		}
		return datetime;
	}
	
	
	public static String getPublishDate(){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String time = bucket.get();
		//return time.split(" ")[0];
		return getDate(time, "yyyy-MM-dd");
	}
	
	public static String getDate(String timestamp, String pattern){
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Date date = null;
		try {
			date = sdf.parse(timestamp);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String str = sdf.format(date);
		return str;
	}
}
