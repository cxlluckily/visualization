package com.shankephone.data.visualization.web.ticket;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.redisson.api.RScoredSortedSet;
import org.redisson.client.protocol.ScoredEntry;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

public class TicketPaymentProcessor implements SubDataInitProcessor {

	@Override
	public Object process(SubInfo subInfo) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		String fmt = sdf.format(date);
		String topics = subInfo.getTopic();
		String redissonNameSpace=PropertyAccessor.getProperty("redis.topic.realtime.tickets.payment.namespace");
		RScoredSortedSet<String> scoredSortedSet = RedisUtils.getRedissonClient().getScoredSortedSet(redissonNameSpace+topics+"_"+fmt);
		JSONArray nameArr = new JSONArray();
		JSONArray dataArr = new JSONArray();
		Collection<ScoredEntry<String>> entryRangeReversed = scoredSortedSet.entryRangeReversed(0, scoredSortedSet.size());
		for(ScoredEntry<String> t : entryRangeReversed){
			nameArr.add(t.getValue());
			JSONObject tmp = new JSONObject();
			tmp.put("name", t.getValue());
			tmp.put("value", t.getScore());
			dataArr.add(tmp);
		}
		JSONObject result = new JSONObject();
		result.put("rows", dataArr);
		result.put("names", nameArr);
		
		return result;
	}

}
