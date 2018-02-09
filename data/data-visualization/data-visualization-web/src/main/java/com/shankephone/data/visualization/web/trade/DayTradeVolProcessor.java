package com.shankephone.data.visualization.web.trade;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

public class DayTradeVolProcessor implements SubDataInitProcessor {
	
	private final static Logger logger = LoggerFactory.getLogger(DayTradeVolProcessor.class);
	
	@Override
	public Object process(SubInfo subInfo) {
		JSONObject json = new JSONObject();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		RedissonClient redisson = RedisUtils.getRedissonClient();
		String keyPrefix=subInfo.getTopic()==null?"":subInfo.getTopic();
		if(null==keyPrefix||"".equals(keyPrefix)){
			return json;
		}
		/*RBucket<String> bucket = redisson.getBucket("lastTime");
		String time = bucket.get();
		if(time == null || "".equals(time)){
			return json;
		}
		String current_date = time.split(" ")[0];*/
		String current_date=sdf.format(new Date());//临时使用
		RMap<String, JSONObject> map=redisson.getMap(keyPrefix);
		Map<String, Object> resultMap=map.get(current_date);
		if(resultMap!=null){
			for(String key : resultMap.keySet()){
				json.put(key, resultMap.get(key));
			}
		}
		logger.info("=================topic:" + keyPrefix + "=================");
		logger.info("@date:@"+current_date+"@返回数据:@"+json);
		return json;
	}

}
