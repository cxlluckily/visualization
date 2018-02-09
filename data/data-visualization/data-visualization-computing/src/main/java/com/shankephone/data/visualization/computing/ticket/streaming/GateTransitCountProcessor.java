package com.shankephone.data.visualization.computing.ticket.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.redisson.api.RMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingProcessor;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.visualization.computing.common.util.LastTimeUtils;

import scala.Tuple2;

public class GateTransitCountProcessor implements SparkStreamingProcessor {
	
	public static final String GATETRANSIT_REDIS_KEY = "gate:vol:4401";
	
	private final static Logger logger = LoggerFactory.getLogger(GateTransitCountProcessor.class);
	
	@Override
	public void process(JavaRDD<JSONObject> rdd, Map<String, String> args) {
		Map<String, Long> countMap = rdd.filter(record -> {
			String tableName = record.getString("tableName");
			if (!tableName.equals("SKP:DATA_YPT_TRAN")) {
				return false;
			}
			JSONObject columns = record.getJSONObject("columns");
			String tranType = columns.getString("TRAN_TYPE");
			if (tranType.equals("53") || tranType.equals("54") ||
				tranType.equals("62") || tranType.equals("63") ||
				tranType.equals("73") || tranType.equals("74") ||
				tranType.equals("90") || tranType.equals("91")) {
				return true;
			}
			return false;
		}).mapToPair(record -> {
			JSONObject columns = record.getJSONObject("columns");
			String productCategory = columns.getString("PRODUCT_CATEGORY");
			String tranDate = columns.getString("TRAN_DATE");
			return new Tuple2<String, Integer>(tranDate + "_" + productCategory, 1);
		}).countByKey();
		
		Map<String, Map<String, Integer>> dateMap = new HashMap<>();
		for (Entry<String, Long> entry : countMap.entrySet()) {
			String date = entry.getKey().split("_")[0];
			date = DateUtils.convertDateStr(date, "yyyyMMdd");
			String type = entry.getKey().split("_")[1];
			Integer count = entry.getValue().intValue();
			Map<String, Integer> dateCountMap = dateMap.get(date);
			if (dateCountMap == null) {
				dateCountMap = new HashMap<>();
				dateMap.put(date, dateCountMap);
			}
			Integer allCount = dateCountMap.get("all");
			if (allCount == null) {
				allCount = 0;
			}
			allCount = allCount + count;
			dateCountMap.put(getTypeName(type), count);
			dateCountMap.put("all", allCount);
		}
		
		for (Entry<String, Map<String, Integer>> dateEntry : dateMap.entrySet()) {
			RMap<String, Map<String, Integer>> rmap = RedisUtils.getRedissonClient().getMap(GATETRANSIT_REDIS_KEY);
			Map<String, Integer> dateCountMap = rmap.get(dateEntry.getKey());
			if (dateCountMap == null) {
				dateCountMap = dateEntry.getValue();
			} else {
				for (Entry<String, Integer> countEntry : dateEntry.getValue().entrySet()) {
					Integer count = dateCountMap.get(countEntry.getKey());
					if (count == null) {
						count = 0;
					}
					dateCountMap.put(countEntry.getKey(), (count + countEntry.getValue()));
				}
			}
			rmap.put(dateEntry.getKey(), dateCountMap);
			String lastTime = LastTimeUtils.getLastTimeDate();
			if (dateEntry.getKey().equals(lastTime)) {
				RedisUtils.getRedissonClient().getTopic(GATETRANSIT_REDIS_KEY).publish(JSONObject.toJSONString(dateCountMap));
			}
		}
	}
	
	public String getTypeName(String type) {
		switch (type) {
			case "01" : return "dcp";//单程票
			case "02" : return "czp";//储值票
			case "03" : return "ccp";//乘次票
			case "05" : return "jnp";//纪念票
			case "07" : return "ygp";//员工票
			case "08" : return "qrcode";//二维码
			case "09" : return "yct";//羊城通
			case "10" : return "ic";//金融IC卡
			case "11" : return "cloud";//地铁云卡
			default : return "other";//其他
		}
	}
}
