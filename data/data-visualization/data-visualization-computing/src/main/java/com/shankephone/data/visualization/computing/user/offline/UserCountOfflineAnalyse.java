package com.shankephone.data.visualization.computing.user.offline;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.computing.Main;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.util.ClassUtils;
import com.shankephone.data.common.util.FreemarkerUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;

/**
 * 离线计算今日用户数
 * @author 森
 * @version 2017年9月20日 下午3:52:46
 */
public class UserCountOfflineAnalyse implements Executable {

	private final static Logger logger = LoggerFactory.getLogger(UserCountOfflineAnalyse.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	public void execute(Map<String, String> argsMap) {
		analyseUserNum("user_count", argsMap.get("endTime"));
		
	}

	private void analyseUserNum(String sqlFile, String endTime) {
		logger.info("=========================start==========================");
		Date date = new Date();
		Date end = date;
		//如果指定时间
		if(endTime != null && !"".equals(endTime)){
			//设置指定时间为终点
			endTime = IAnalyseHandler.getLongTimestamp(endTime);
			Long timestamp = Long.parseLong(endTime);
			end = new Date(timestamp);
		} 
		Calendar cal = Calendar.getInstance();
		cal.setTime(end);
		String current_date = sdf.format(cal.getTime());
		long stopTimestamp = cal.getTimeInMillis();
	
		logger.info("数据处理中......");
		Map<String,Object> params = new HashMap<String,Object>();
		params.put("current_date", "'" +current_date + "'");
		params.put("stopTimestamp", "'" + stopTimestamp + "'");
		
		SparkSQLBuilder builder = new SparkSQLBuilder("user_count");
		Dataset<Row> results = builder.executeSQL(sqlFile, params);
		
		List<Row> rows = results.collectAsList();
		Map<String, Set<String>> city_users = new HashMap<>();
		for(Row row : rows){
			String city_code = row.getString(0);
			String pay_account = row.getString(1);
			
			Set<String> users = city_users.get(city_code);
			if(users==null){
				users = new HashSet<String>();
			}
			users.add(pay_account);
			city_users.put(city_code, users);
		}
		String namespace = PropertyAccessor.getProperty("redis.topic.realtime.user.count.namespace");
		String key = PropertyAccessor.getProperty("redis.topic.realtime.user.count.key");
		RedissonClient redisson = RedisUtils.getRedissonClient();
		for (String city_code : city_users.keySet()){
			RSet<String> set = redisson.getSet(namespace+key+"-"+city_code+"_"+current_date);
			set.clear();
			for (String user : city_users.get(city_code))
				set.add(user);
			set.expire(2, TimeUnit.DAYS);
			
			//publish uc-(city_code)
			String city_name = null;
			switch(city_code){
				case "1200":
					city_name="天津";
					break;
				case "2660":
					city_name = "青岛";
					break;
				case "4100":
					city_name = "长沙";
					break;
				case "4401":
					city_name = "广州";
					break;
				case "4500":
					city_name = "郑州";
					break;
				case "5300":
					city_name = "南宁";
					break;
				case "7100":
					city_name = "西安";
					break;
				default:
					city_name = "广州";
			}
			JSONObject result = new JSONObject();
			result.put("CITY_CODE", city_code);
			result.put("CITY_NAME", city_name);
			result.put("USER_COUNT", set.size());
			redisson.getTopic(key+"-"+city_code).publish(result.toJSONString());
		}
		builder.getSession().close();
		logger.info("=========================end==========================");
	}

}
