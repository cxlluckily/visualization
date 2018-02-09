package com.shankephone.data.visualization.web.ticket;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

/**
 * 近30日售票量初始化
 * @author 森
 * @version 2017年9月15日 下午4:21:00
 */

public class MonthTicketsProcessor implements SubDataInitProcessor {
	public final static Logger logger = LoggerFactory.getLogger(MonthTicketsProcessor.class); 
	@Override
	public Object process(SubInfo subInfo) {
		String topics = subInfo.getTopic();
		String city_code = topics.split(":")[2];

		JSONObject tickets_30days = new JSONObject();
		List<String> time = new ArrayList<String>();
		List<Integer> tickets = new ArrayList<Integer>();
				
		RedissonClient redisson = RedisUtils.getRedissonClient();
		String namespace=PropertyAccessor.getProperty("redis.topic.realtime.tickets.total.namespace");
		String KEY = PropertyAccessor.getProperty("redis.topic.realtime.tickets.total.key");
		Map<String, JSONObject> map = redisson.getMap(namespace+KEY);
		JSONObject dayValue = map.get(city_code);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String lasttime = bucket.get();
		if(lasttime == null || "".equals(lasttime)){
			return new JSONObject();
		}
		Date today = null;
		try {
			today = sdf.parse(lasttime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		calendar.setTime(today);
		calendar.add(Calendar.DATE, -30);
		String before = sdf.format(calendar.getTime());
		int i = 0;
		while (i<30) {
			tickets.add(dayValue.getIntValue(before));
			time.add(before);
			calendar.add(Calendar.DATE, 1);
			before = sdf.format(calendar.getTime());
			i++;
		}
		tickets_30days.put("TIME", time);
		tickets_30days.put("TICKETS", tickets);
		logger.info("【tickets:30days:"+city_code+"】"+tickets_30days);
		return tickets_30days;
	}

}
