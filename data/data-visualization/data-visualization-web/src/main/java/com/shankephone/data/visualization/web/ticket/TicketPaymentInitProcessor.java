package com.shankephone.data.visualization.web.ticket;

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

public class TicketPaymentInitProcessor implements SubDataInitProcessor {

	private static final String SEPERATOR = "_";
	private final static Logger logger = LoggerFactory.getLogger(TicketPaymentInitProcessor.class);
	
	@Override
	public Object process(SubInfo subInfo) {
		//获取今天的日期字符串
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String time = bucket.get();
		if(time == null || "".equals(time)){
			return new JSONObject();
		}
		String today = time.split(" ")[0];
		
		//从前端输入的topic参数格式为：topicname 或 topicname_citycode
		String topics = subInfo.getTopic();
		
		String [] array = topics.split(SEPERATOR);
		String topic = array[0];
		String city_code = "0000";
		if(array.length > 1){
			city_code = array[1];
		}
		topic = "ticket:offline:payment:" + topic + SEPERATOR + today;
		System.out.println(topic);
		RMap<String,JSONObject> map = RedisUtils.getRedissonClient()
				.getMap(topic);
		JSONObject result = new JSONObject();
		JSONObject json = map.get(city_code);
		if(json != null){
			JSONArray arr = new JSONArray();
			JSONArray names = new JSONArray();
			try {
				for(String code : json.keySet()){
					JSONObject pj = json.getJSONObject(code);
					if(pj != null){
						arr.add(pj);
						String name = pj.getString("name");
						names.add(name);
					}
				}
				result.put("names", names);
				result.put("rows", arr);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("error info: parse json string is error!");
				return null;
			}
		}
		logger.info(result.toJSONString());
		return result;
	}
	
	
}
