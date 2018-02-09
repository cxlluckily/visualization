package com.shankephone.data.visualization.computing.user.offline;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;
import com.shankephone.data.visualization.computing.common.util.Constants;

/**
 * 新老用户离线分析
 * @author fengql
 * @version 2018年2月2日 下午3:55:55
 */
public class UserActiveAnalyse implements Executable{

	private final static Logger logger = LoggerFactory.getLogger(UserActiveAnalyse.class);
	
	@Override
	public void execute(Map<String, String> argsMap) {
		logger.info("======================CityUserActiveOfflineAnalyse start===========================");
		String timestamp = argsMap == null ? null : argsMap.get("endTime");
		SparkSQLBuilder builder = new SparkSQLBuilder("user_action_offline");
		analyseActivityUsers(builder, "user_new_register", Constants.ANALYSE_USER_NUM_NEW, timestamp);
		analyseActivityUsers(builder, "user_old_payment", Constants.ANALYSE_USER_NUM_OLD, timestamp);
		builder.getSession().close();
		logger.info("======================CityUserActiveOfflineAnalyse end===========================");
	}
	
	public void analyseActivityUsers(SparkSQLBuilder builder,  String sqlFile, String category, String timestamp){
		try {
			//t_last_timestamp的时间戳
			String endtime = IAnalyseHandler.getLongTimestamp(timestamp);
			if(endtime == null || "".equals(endtime)){
				Date date = new Date();
				endtime = date.getTime() + "";
			}
			Long end = Long.parseLong(endtime);
			//前一天的最后时刻时间戳
			String endtimestamp = IAnalyseHandler.getLongTimestamp(timestamp, 1);
			long dataend = Long.parseLong(endtimestamp);
			Date date = new Date(dataend);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.add(Calendar.DAY_OF_YEAR, -30);
			//pay_pay_time数据时间限制
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("lastTime", end + "");
			params.put("startDay", "'" + sdf.format(cal.getTime()) + "'");
			params.put("endDay", "'" + sdf.format(date.getTime()) + "'");
			Dataset<Row> results = builder.executeSQL(sqlFile, params);
			reduceByPeriodType(end, results, category);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	/**
	 * 按时间维度合并计算
	 * @author fengql
	 * @date 2017年8月22日 下午4:58:34
	 * @param periodType
	 * @param dataset
	 * @param category
	 */
	private void reduceByPeriodType(Long end,
			 Dataset<Row> dataset, String category) {
		Date date = new Date(end);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		//今天的时间与上面的date不是同一天，date是昨天
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String, String> rmap = redisson.getMap(
				Constants.REDIS_NAMESPACE_USER_ACTIVE
				);
		//生成list结果集
		List<Row> rows = dataset.collectAsList();
		//key:城市_用户类型,value:JSONObject<day, count>的map
		Map<String, JSONObject> map = new HashMap<String,JSONObject>();
		
		//每个城市-用户类型计算结果中的日期列表
		Map<String,Set<String>> daySetMap = new HashMap<String,Set<String>>();
		for(Row r : rows){
			String city_code = r.getString(0);
			String dates = r.getString(1);
			String day = dates.split(" ")[0];
			Long count = r.getLong(2);
			String key = city_code + Constants.DIMENSION_SEPERATOR + category;
			//存放day和count的JSONObject
			JSONObject json = map.get(key);
			if(json == null){
				json = new JSONObject();
			}
			json.put(day, count.intValue());
			map.put(key, json);
			Set<String> daySet = daySetMap.get(key);
			if(daySet == null){
				daySet = new HashSet<String>();
			}
			//放入计算出的天
			daySet.add(day);
			daySetMap.put(key, daySet);
		}
		
		//生成30天的日期
		List<String> list = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		for(int i = 0; i <= 30; i++){
			cal.add(Calendar.DAY_OF_YEAR, -1);
			Date day = cal.getTime();
			String days = sdf.format(day);
			list.add(days);
		}
		Collections.reverse(list);
		JSONArray xaxis = new JSONArray();
		for(String day : list){
			xaxis.add(day);
		}
		
		Map<String, JSONObject> allMap = new HashMap<String, JSONObject>();
		//遍历30天日期，补齐并数据
		for(String day : list){
			Set<String> keySet = map.keySet();
			for(String key : keySet){
				//取出JSON
				JSONObject json = map.get(key);
				if(json == null){
					continue;
				}
				Set<String> days = daySetMap.get(key);
				//如果没有当前日期，补齐数据，放回map
				if(!days.contains(day)){
					json.put(day, 0);
					map.put(key, json);
				}
				int val = json.getIntValue(day);
				//取出汇总的JSON
				JSONObject allJson = allMap.get("all" + 
						Constants.DIMENSION_SEPERATOR + category);
				if(allJson == null){
					allJson = new JSONObject();
				}
				int allVal = allJson.getIntValue(day);
				allJson.put(day, allVal + val);
				allMap.put("all" + 
						Constants.DIMENSION_SEPERATOR + category, allJson);
			}
		}
		
		
		//遍历城市-用户类型的map，并放入缓存	
		Set<String> keySet = map.keySet();
		for(String key : keySet){
			JSONObject json = map.get(key);
			JSONObject js = sortJSONObject(json);
			String city_code = key.split(Constants.DIMENSION_SEPERATOR)[0];
			String cacheName = Constants.REDIS_NAMESPACE_USER_ACTIVE;
			//写入缓存
			rmap.put(key, js.toJSONString());
			JSONObject o = getAllUserActive(cacheName, city_code);
			if(o != null){
				redisson.getTopic(
						Constants.REDIS_NAMESPACE_USER_ACTIVE
						).publish(o.toJSONString());
			} 
		}
		//遍历all-用户类型的map，并放入缓存
		Set<String> allKeyset = allMap.keySet();
		for(String key : allKeyset){
			JSONObject json = allMap.get(key);
			JSONObject js = sortJSONObject(json);
			//写入缓存
			rmap.put(key, js.toJSONString());
			String cacheName = Constants.REDIS_NAMESPACE_USER_ACTIVE ;
			JSONObject o = getAllUserActive(cacheName, "all"); 
			if(o != null){
				redisson.getTopic(
						Constants.REDIS_NAMESPACE_USER_ACTIVE					
						).publish(o.toJSONString());
			}
		}
	}
	
	private JSONObject getAllUserActive(String cacheName ,String city_code){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String, String> rm = redisson.getMap(cacheName);
		String oldstr = rm.get(city_code + Constants.DIMENSION_SEPERATOR + "2200");
		String newstr = rm.get(city_code + Constants.DIMENSION_SEPERATOR + "2100");
		JSONObject json = new JSONObject();
		JSONArray xaxis = new JSONArray();
	
		JSONObject oldjson = new JSONObject();
		if(oldstr != null && !"".equals(oldstr)){ 
			oldjson = JSONObject.parseObject(oldstr);
			//重排旧用户数据日期
			Set<String> oldset = oldjson.keySet();
			if(oldset != null && oldset.size() >0){
				List<String> oldlist = new ArrayList<String>();
				for (Iterator<String> it = oldset.iterator(); it.hasNext();) {
					String key = it.next();
					oldlist.add(key);
				}
				Collections.sort(oldlist);
				JSONArray oldArray = new JSONArray();
				for (int i = 0; i < oldlist.size(); i++) {
					String day = oldlist.get(i);
					oldArray.add(oldjson.get(day));
				}
				json.put("old", oldArray);
			}
			
		}
		
		JSONObject newjson = new JSONObject();
		if(newstr != null && !"".equals(newstr)){ 
			newjson = JSONObject.parseObject(newstr);
			//重排新用户数据日期
			Set<String> newset = newjson.keySet();
			if(newset != null && newset.size() > 0){
				List<String> newlist = new ArrayList<String>();
				for (Iterator<String> it = newset.iterator(); it.hasNext();) {
					String key = it.next();
					newlist.add(key);
				}
				Collections.sort(newlist);
				JSONArray newArray = new JSONArray();
				for (int i = 0; i < newlist.size(); i++) {
					String day = newlist.get(i);
					newArray.add(newjson.get(day));
					xaxis.add(day);
				}
				json.put("new", newArray);
			}
		}
		json.put("xaxis", xaxis);
			
		
		return json;
	}

	private JSONObject sortJSONObject(JSONObject json) {
		Set<String> jsonKeys = json.keySet();
		List<String> keyList = new ArrayList<String>();
		for(String jsonKey : jsonKeys){
			keyList.add(jsonKey);
		}
		Collections.sort(keyList);
		JSONObject js = new JSONObject();
		for(String k : keyList){
			js.put(k, json.get(k));
		}
		return js;
	}
}
