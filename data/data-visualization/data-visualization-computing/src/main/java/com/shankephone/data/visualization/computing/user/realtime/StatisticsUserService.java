
package com.shankephone.data.visualization.computing.user.realtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.apache.commons.lang3.time.FastDateFormat;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;

/**
 * @todo 实时出票统计业务类
 * @author sunweihong
 * @version 2017年8月4日 上午10:54:06
 */
public class StatisticsUserService {
	
	private RedissonClient redisson;
	/**
	 * @todo 单例保存对象
	 */
	private static StatisticsUserService singleton;  
	
	private long tLastTimestamp=0l;
	
	/**
	 * @todo 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(StatisticsUserService.class);
	
	
	private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
	
	private ApplicationContext appContext = null;
	
	
	private StatisticsUserService() {
		if(appContext==null){
			appContext =  new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
		}
		redisson = (RedissonClient) appContext.getBean("redissonClient");
	}
	
	
	
	/** 
	 * @todo 之所以使用单例，是不想多次加载配置文件，以提高速度，而且当前业务足够用
	 * @author sunweihong
	 * @date 2017年6月8日 上午10:10:06
	 * @return
	 */
	public static StatisticsUserService getInstance(){
		if (singleton == null) {  
			singleton = new StatisticsUserService();  
		}  
		return singleton;  
	}
	
	/** 
	 * @todo 统计用户数量
	 * @author sunweihong
	 * @date 2017年8月28日 下午2:06:24
	 * @param value
	 */
	public void startStatisticsUserCount(JSONObject value) {
		try {
			JSONObject columns = value.getJSONObject("columns");
			String city_code = columns.getString("CITY_CODE");
			String user_count_topic = "";
			String city_name = "";
			String currentDay = "";
			String redissonNameSpace=PropertyAccessor.getProperty("redis.topic.realtime.user.count.namespace");
			String user_count_total_topic=PropertyAccessor.getProperty("redis.topic.realtime.user.count.total");
			
			switch(city_code){
			case "1200":
				user_count_topic=PropertyAccessor.getProperty("redis.topic.realtime.user.count.1200");
				city_name="天津";
				break;
			case "2660":
				user_count_topic = PropertyAccessor.getProperty("redis.topic.realtime.user.count.2660");
				city_name = "青岛";
				break;
			case "4100":
				user_count_topic = PropertyAccessor.getProperty("redis.topic.realtime.user.count.4100");
				city_name = "长沙";
				break;
			case "4401":
				user_count_topic = PropertyAccessor.getProperty("redis.topic.realtime.user.count.4401");
				city_name = "广州";
				break;
			case "4500":
				user_count_topic = PropertyAccessor.getProperty("redis.topic.realtime.user.count.4500");
				city_name = "郑州";
				break;
			case "5300":
				user_count_topic = PropertyAccessor.getProperty("redis.topic.realtime.user.count.5300");
				city_name = "南宁";
				break;
			case "7100":
				user_count_topic = PropertyAccessor.getProperty("redis.topic.realtime.user.count.7100");
				city_name = "西安";
				break;
			default:
				user_count_topic = PropertyAccessor.getProperty("redis.topic.realtime.user.count.4401");
				city_name = "广州";
			}
			String pay_pay_time = columns.getString("PAY_PAY_TIME");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(fastDateFormat.parse(pay_pay_time));
			currentDay = fastDateFormat.format(calendar);

			RKeys keys = redisson.getKeys();
			if(keys.countExists(redissonNameSpace+user_count_topic+"_"+currentDay) == 0){
				RSet<String> set = redisson.getSet(redissonNameSpace+user_count_topic+"_"+currentDay);
				RSet<String> setTotal = redisson.getSet(redissonNameSpace+user_count_total_topic+"_"+currentDay);
				set.expire(7,TimeUnit.DAYS);
				setTotal.expire(7,TimeUnit.DAYS);
			}
			RSet<String> set = redisson.getSet(redissonNameSpace+user_count_topic+"_"+currentDay);
			RSet<String> setTotal = redisson.getSet(redissonNameSpace+user_count_total_topic+"_"+currentDay);
			if(!set.contains(columns.getString("PAY_PAY_ACCOUNT"))){
				set.add(columns.getString("PAY_PAY_ACCOUNT"));
				JSONObject result = new JSONObject();
				result.put("CITY_CODE", city_code);
				result.put("CITY_NAME", city_name);
				result.put("USER_COUNT", set.size());
				//TODO 待处理，目前存入redis
				RedisUtils.getRedissonClient()
		        .getTopic(user_count_topic)
		        .publish(result.toJSONString());
				logger.info("【"+user_count_topic+"】发布成功! ");
			}
			
			if(!setTotal.contains(columns.getString("PAY_PAY_ACCOUNT"))){
				setTotal.add(columns.getString("PAY_PAY_ACCOUNT"));
				JSONObject resultTotal = new JSONObject();
				resultTotal.put("CITY_CODE", "all");
				resultTotal.put("CITY_NAME", "全国");
				resultTotal.put("USER_COUNT", setTotal.size());
				
				//TODO 待处理，目前存入redis
				RedisUtils.getRedissonClient()
		        .getTopic(user_count_total_topic)
		        .publish(resultTotal.toJSONString());
				logger.info("【"+user_count_total_topic+"】发布成功! ");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("【UsersCount】更新失败! ");
		}
		
		logger.info("【UsersCount】更新成功! ");
	}
	

	
	/** 
	 * @todo 过滤有用的用户信息
	 * @author sunweihong
	 * @date 2017年8月4日 上午10:16:09
	 * @param value
	 * @return
	 */
	public boolean filterUserUsefulData(JSONObject value){
		
		JSONObject columns = value.getJSONObject("columns");
		boolean timestampFlag = columns.containsKey("T_LAST_TIMESTAMP");
		if(tLastTimestamp!=0l){
			Long exceptTimestamp= tLastTimestamp;
			Long actualTimestamp= Long.parseLong(columns.getString("T_LAST_TIMESTAMP"));
//			logger.info("【RT-TICKET】设定timestamp"+exceptTimestamp+"，现在timestamp"+actualTimestamp);
			if(actualTimestamp<=exceptTimestamp){
				timestampFlag = timestampFlag && false;
			}
		}
		
		boolean fileNameFlag = columns.containsKey("ORDER_TYPE") && columns.containsKey("TICKET_ORDER_STATUS")&& columns.containsKey("PAY_PAY_ACCOUNT")&& columns.containsKey("PAY_PAY_TIME")&& columns.containsKey("CITY_CODE");
		boolean statusFlag = "1".equals(columns.getString("ORDER_TYPE")) && "5".equals(columns.getString("TICKET_ORDER_STATUS")) && !"".equals(columns.getString("PAY_PAY_ACCOUNT")) && !"".equals(columns.getString("PAY_PAY_TIME"))&& !"".equals(columns.getString("CITY_CODE"));
		if(timestampFlag && fileNameFlag && statusFlag){
//			logger.info("【UsersCount】当前数据符合条件，需要处理! ");
			return true;
		}
		return false;
	}



	public long gettLastTimestamp() {
		return tLastTimestamp;
	}



	public void settLastTimestamp(long tLastTimestamp) {
		this.tLastTimestamp = tLastTimestamp;
	}
	
	

}
