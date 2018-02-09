package com.shankephone.data.visualization.computing.user.streaming;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.computing.Main;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingBuilder;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;

import scala.Tuple2;


/**
 * 实时计算城市用户数
 * @author 森
 * @version 2017年9月20日 上午11:54:12
 */

public class UserCountProcess implements Executable {

	private final static Logger logger = LoggerFactory.getLogger(UserCountProcess.class);
	
	@Override
	public void execute(Map<String, String> argsMap) {
		
		SparkStreamingBuilder builder = new SparkStreamingBuilder("userCount");
		JavaInputDStream<ConsumerRecord<String, String>> stream;
		long t_last_timestamp;
		if (argsMap==null||argsMap.get("startTime")==null){
			stream = builder.createKafkaStream();
			t_last_timestamp = 0l;
		}
		else{
			t_last_timestamp = Long.parseLong(IAnalyseHandler.getLongTimestamp(argsMap.get("startTime")));
			stream = builder.createKafkaStream(t_last_timestamp);
		}
		
		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			//计算
			JavaPairRDD<String, String> results = rdd.filter(f->{
				JSONObject value = JSONObject.parseObject(f.value());
				return filterUserData(value, t_last_timestamp);
			}).mapToPair(f->{
				JSONObject value = JSONObject.parseObject(f.value());
				JSONObject columns = value.getJSONObject("columns");
				
				String city_code = columns.getString("CITY_CODE");
				String pay_time = columns.getString("PAY_PAY_TIME");
				String pay_account = columns.getString("PAY_PAY_ACCOUNT");
				
				return new Tuple2<>(city_code+"_"+pay_time, pay_account);
			}).reduceByKey((a,b)->a+"//"+b);
			//输出
			List<Tuple2<String, String>>  records = results.collect();
			for (Tuple2<String, String> record : records){
				String[] keys =  record._1.split("_");
				String[] pay_accounts = record._2.split("//");
				String city_code = keys[0];
				String pay_time = keys[1];
				userCount(city_code, pay_time, pay_accounts);
			}
			
			((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
		});

		try {
			builder.getStreamingContext().start();
			builder.getStreamingContext().awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean filterUserData(JSONObject value, long timestamp){
		JSONObject columns = value.getJSONObject("columns");
		boolean timestampFlag = columns.containsKey("T_LAST_TIMESTAMP");
		if(timestamp!=0l){
			Long exceptTimestamp= timestamp;
			Long actualTimestamp= Long.parseLong(columns.getString("T_LAST_TIMESTAMP"));
			if(actualTimestamp<=exceptTimestamp){
				timestampFlag =  false;
			}
		}
		
		boolean fileNameFlag = columns.containsKey("ORDER_TYPE") && columns.containsKey("TICKET_ORDER_STATUS")&& columns.containsKey("PAY_PAY_ACCOUNT")&& columns.containsKey("PAY_PAY_TIME")&& columns.containsKey("CITY_CODE");
		boolean statusFlag = "1".equals(columns.getString("ORDER_TYPE")) && "5".equals(columns.getString("TICKET_ORDER_STATUS")) && !"".equals(columns.getString("PAY_PAY_ACCOUNT")) && !"".equals(columns.getString("PAY_PAY_TIME"))&& !"".equals(columns.getString("CITY_CODE"));
		if(timestampFlag && fileNameFlag && statusFlag){
			return true;
		}
		return false;
	}

	public static void userCount(String city_code, String pay_time, String[] pay_accounts) throws ParseException {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		
		String namespace = PropertyAccessor.getProperty("redis.topic.realtime.user.count.namespace");
		String key = PropertyAccessor.getProperty("redis.topic.realtime.user.count.key");
		String pay_date = pay_time.split(" ")[0];
		
		RKeys keys = redisson.getKeys();
		boolean setExpireFlag = false;
		RSet<String> set = redisson.getSet(namespace+key+"-"+city_code+"_"+pay_date);
		if(keys.countExists(namespace+key+"-"+city_code+"_"+pay_date) == 0)
			setExpireFlag = true;
		for(String pay_account : pay_accounts)
			set.add(pay_account);
		if(setExpireFlag) 
			set.expire(2,TimeUnit.DAYS);
		
		/**publish uc-(city_code)**/
		String last_date = "";

		RBucket<String> bucket = redisson.getBucket("lastTime");
		last_date = bucket.get().split(" ")[0];
		//是当天数据才推送
		if(last_date.equals(pay_date)) {
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
	}
	
//	public static void main(String[] args){
//		String[] a = new String[]{"-mainClass","com.shankephone.data.visualization.computing.user.streaming.UserCountProcess","-argument","startTime=2017-09-26 13:50:00"};
//		Main.main(a);
//	}

}
