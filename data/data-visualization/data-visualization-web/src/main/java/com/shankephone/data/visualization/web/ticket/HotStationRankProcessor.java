package com.shankephone.data.visualization.web.ticket;

import java.util.Collection;

import org.redisson.api.RBucket;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

public class HotStationRankProcessor implements SubDataInitProcessor{

	@Override
	public Object process(SubInfo subInfo) {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> timeBucket = redisson.getBucket("lastTime");
		String time = timeBucket.get();
		if(time == null || "".equals(time)){
			return new JSONObject();
		}
		String current_date = time.split(" ")[0];
		String topics = subInfo.getTopic();
		String city_code = topics.substring(topics.lastIndexOf("-") + 1);
		RBucket<String> bucket =  redisson.getBucket(PropertyAccessor.getProperty("redis.load.citylinestationrelations.key"));
		JSONObject cityLineStationRelations =  JSON.parseObject(bucket.get());
		String redissonNameSpace=PropertyAccessor.getProperty("redis.topic.realtime.tickets.hsrank.namespace");
		RScoredSortedSet<String> set = redisson.getScoredSortedSet(redissonNameSpace+topics+"_"+current_date);
		Collection<ScoredEntry<String>> entryRangeReversed = set.entryRangeReversed(0, 9);
		int i=0;
		JSONArray currentTop10 = new JSONArray();
		for(ScoredEntry<String> t : entryRangeReversed){
			i++;
			String rank_station_name = cityLineStationRelations.getJSONObject(city_code).getJSONObject(t.getValue()).getString("STATION_NAME_ZH");
			JSONObject rank = new JSONObject();
			rank.put("RANK", "NO"+i);
			rank.put("TICKET_COUNT", t.getScore().longValue());
			rank.put("STATION_NAME", rank_station_name);
			currentTop10.add(rank);
		}
		return currentTop10;
	}

}
