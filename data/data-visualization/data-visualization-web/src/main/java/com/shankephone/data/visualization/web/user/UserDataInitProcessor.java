package com.shankephone.data.visualization.web.user;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

public class UserDataInitProcessor  implements SubDataInitProcessor {

	private static final String SEPERATOR = "_";
	private final static Logger logger = LoggerFactory.getLogger(UserDataInitProcessor.class);
	
	@Override
	public Object process(SubInfo subInfo) {
		JSONObject json = null;
		
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String time = bucket.get();
		if(time == null || "".equals(time)){
			return new JSONObject();
		}
		String topics = subInfo.getTopic();
		//从前端输入的topic参数格式为：topicname 或 topicname_citycode
		
		String city_code = "all";
		//新老用户
		String [] tps = topics.split("_");
		String topic = null;
		if(tps.length == 1){
			topic = topics;
		} else {
			topic = tps[0];
			city_code = tps[1];
		}
		json = getAllUserActive(topic, city_code);
		logger.info("=================topic: " + topic + "=================");
		logger.info(json.toJSONString());
		
		json = getAllUserActive(topic, city_code);
		return json;
	}
	
	
	
	/**
	 * 新老用户
	 * @author fengql
	 * @date 2017年8月31日 下午2:44:04
	 * @param topic
	 * @param city_code
	 * @return
	 */
	private JSONObject getAllUserActive(String topic ,String city_code){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String, String> rm = redisson.getMap(topic);
		String oldstr = rm.get(city_code + SEPERATOR + "2200");
		JSONObject oldjson = JSONObject.parseObject(oldstr);
		String newstr = rm.get(city_code + SEPERATOR + "2100");
		JSONObject newjson = JSONObject.parseObject(newstr);
		
		JSONObject json = new JSONObject();
		try {
			if(oldjson != null){
				//重排旧用户数据日期
				Set<String> oldset = oldjson.keySet();
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
			
			
			JSONArray xaxis = new JSONArray();
			if(newjson != null){
				//重排新用户数据日期
				Set<String> newset = newjson.keySet();
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
				json.put("xaxis", xaxis);
				json.put("new", newArray);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("========user active data error: " + e.getMessage() + "======");
			return null;
		}
		System.out.println(json.toJSONString());
		return json;
	}

}
