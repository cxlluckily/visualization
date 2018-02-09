package com.shankephone.data.visualization.web.user;

import org.redisson.api.RBucket;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

public class UserCountOfCityProcessor implements SubDataInitProcessor{

	@Override
	public Object process(SubInfo subInfo) {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> timeBucket = redisson.getBucket("lastTime");
		String lasttime = timeBucket.get();
		if(lasttime == null || "".equals(lasttime)){
			return new JSONObject();
		}
		String current_date = timeBucket.get().split(" ")[0];
		String topics = subInfo.getTopic();
		String city_code = topics.substring(topics.lastIndexOf("-") + 1);
		String city_name="";
		switch(city_code){
		case "1200":
			city_name="天津"	;
			break;
		case "2660":
			city_name="青岛"	;
			break;
		case "4100":
			city_name="长沙"	;
			break;
		case "4401":
			city_name="广州"	;
			break;
		case "4500":
			city_name="郑州"	;
			break;
		case "5300":
			city_name="南宁"	;
			break;
		case "7100":
			city_name="西安"	;
			break;
		default:
			city_name="广州"	;
		
		}
		String redissonNameSpace=PropertyAccessor.getProperty("redis.topic.realtime.user.count.namespace");
		RSet<Object> set = redisson.getSet(redissonNameSpace+topics+"_"+current_date);
		JSONObject result = new JSONObject();
		result.put("CITY_CODE", city_code);
		result.put("CITY_NAME", city_name);
		result.put("USER_COUNT", set.size());
		System.out.println("【user】"+result);
		return result;
	}

	

}
