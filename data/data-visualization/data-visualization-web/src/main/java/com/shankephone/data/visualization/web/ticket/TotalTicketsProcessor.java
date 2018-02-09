package com.shankephone.data.visualization.web.ticket;

import java.util.Set;

import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

public class TotalTicketsProcessor implements SubDataInitProcessor {
	public final static Logger logger = LoggerFactory.getLogger(TotalTicketsProcessor.class); 
	@Override
	public Object process(SubInfo subInfo) {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		String namespace=PropertyAccessor.getProperty("redis.topic.realtime.tickets.total.namespace");
		String KEY = subInfo.getTopic().split("_")[0];
		RMap<String, JSONObject> map = redisson.getMap(namespace+KEY);
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String time = bucket.get();
		if(time == null || "".equals(time)){
			return new JSONObject();
		}
		String current_date = time.split(" ")[0];
		JSONObject city_ticket = new JSONObject();
		Set<String> keys = map.keySet();
		for(String key : keys){
			String city_name = "";
			switch (key){
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
				city_name = "全国";
			}
			Integer ticket_num = map.get(key).getInteger(current_date);
			city_ticket.put(city_name, ticket_num==null?0:ticket_num);
		}
		if ( city_ticket.size()==0 )
			logger.info("================TotalTicketsProcessor : data missing================");
		logger.info("【全国售票量】"+city_ticket.toJSONString());
		return city_ticket;
	}
	

}
