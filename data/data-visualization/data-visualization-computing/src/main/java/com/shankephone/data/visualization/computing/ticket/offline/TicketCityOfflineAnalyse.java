package com.shankephone.data.visualization.computing.ticket.offline;



import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;
/**
 * 离线 - 城市售票量 与 热门站点
 * @author 森
 * @version 2017年9月19日 下午4:44:49
 */
public class TicketCityOfflineAnalyse implements Executable {
	
	private final static Logger logger = LoggerFactory.getLogger(TicketCityOfflineAnalyse.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	public void execute(Map<String, String> argsMap) {
		analyseTicketNum("ticket_count", argsMap.get("endTime"));
	}

	/**
	 * 离线计算30天和time当日售票量 
	 * @author 森
	 * @date 2017年9月1日 上午11:17:25
	 * @param time 统计结束时间
	 */
	public void analyseTicketNum(String sqlFile, String time){
		logger.info("=========================start==========================");
		Date date = new Date();
		Date end = date;
		//如果指定时间
		if(time != null && !"".equals(time)){
			//设置指定时间为终点
			time = IAnalyseHandler.getLongTimestamp(time);
			Long timestamp = Long.parseLong(time);
			end = new Date(timestamp);
		} 
		Calendar cal = Calendar.getInstance();
		cal.setTime(end);
		String stopDate = sdf.format(cal.getTime());
		long stopTimestamp = cal.getTimeInMillis();
		cal.add(Calendar.DAY_OF_YEAR, -31);
		String startDate = sdf.format(cal.getTime());
		long startTimestamp = cal.getTimeInMillis();
		logger.info("数据处理中......");
		Map<String,Object> params = new HashMap<String,Object>();
		params.put("startTimestamp", "'" + startTimestamp + "'");
		params.put("stopTimestamp", "'" + stopTimestamp + "'");
		
		SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
		Dataset<Row> results = builder.executeSQL(sqlFile, params);
		
		saveToTotalTickets(results);		//城市售票量
		
		builder.getSession().close();
		logger.info("=========================end==========================");
	}
	
	/**
	 * 全国各城市近30天与当天售票量 -> redis
	 * @author 森
	 * @date 2017年9月19日 下午4:52:18
	 * @param results
	 */
	public void saveToTotalTickets(Dataset<Row> results){
		List<Row> rows = results.collectAsList();
		Map<String, JSONObject> offlineData = new HashMap<String, JSONObject>();
		
		for(Row row : rows){
			String city_code = row.getString(0);
			String dateStr = row.getString(1);
			Double ticket_num = row.getDouble(2);
			//存入offline结构
			JSONObject dayData = offlineData.get(city_code);
			JSONObject nationData = offlineData.get("nation");
			if(dayData==null)
				dayData = new JSONObject();
			if(nationData==null)
				nationData = new JSONObject();
			dayData.put(dateStr, ticket_num.intValue());
			Integer total = nationData.getInteger(dateStr);
			if (total==null) total = 0;
			nationData.put(dateStr,  ticket_num.intValue()+total);
			offlineData.put(city_code, dayData);	
			offlineData.put("nation", nationData);
		}
		
		RedissonClient redisson = RedisUtils.getRedissonClient();
		String namespace = PropertyAccessor.getProperty("redis.topic.realtime.tickets.total.namespace");
		String KEY = PropertyAccessor.getProperty("redis.topic.realtime.tickets.total.key");
		
		RMap<String,JSONObject> offlineMap = redisson.getMap(namespace + KEY);
		offlineMap.clear();
		for(String key : offlineData.keySet()){
			offlineMap.put(key, offlineData.get(key));
		}
		publishToTopic(offlineData, redisson);//发布到相应topic
	} 
	
	/**
	 * 发布各城市近30日售票量
	 * @author 森
	 * @date 2017年9月19日 下午4:53:39
	 * @param offlineData
	 * @param redisson
	 */
	public void publishToTopic(Map<String, JSONObject> offlineData, RedissonClient redisson){
		Calendar calendar = Calendar.getInstance();
		Date today = new Date();
		for(String key : offlineData.keySet()){
			JSONObject tickets_30days = new JSONObject();
			List<String> time = new ArrayList<String>();
			List<Integer> tickets = new ArrayList<Integer>();
			calendar.setTime(today);
			calendar.add(Calendar.DATE, -30);
			String before = sdf.format(calendar.getTime());
			int i = 0;
			while (i<30) {
				tickets.add(offlineData.get(key).getIntValue(before));
				time.add(before);
				calendar.add(Calendar.DATE, 1);
				before = sdf.format(calendar.getTime());
				i++;
			}
			tickets_30days.put("TIME", time);
			tickets_30days.put("TICKETS", tickets);
			redisson.getTopic("tickets:30days:"+key)
			   .publish(tickets_30days.toJSONString());
			logger.info("【tickets:30days:"+key+"】"+tickets_30days.toString());
		}
	}
	
	public static void main(String[] args) {
		TicketCityOfflineAnalyse p = new TicketCityOfflineAnalyse();
		Map<String,String> map = new HashMap<String,String>();
		map.put("endTime", "2018-02-05 00:00:00");
		p.execute(map); 
	}
}
