package com.shankephone.data.monitoring.web.device.processor;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.spring.ContextAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;
import com.shankephone.data.monitoring.web.device.service.FailureInfoHistoryService;
import com.shankephone.data.monitoring.web.device.service.FailureInfoPhoenixService;

public class FailureRankProcessor implements SubDataInitProcessor{
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  

	@Override
	public Object process(SubInfo subInfo) {
		String city_code = subInfo.getTopic().split("_")[3];
		Calendar ca = Calendar.getInstance();
		ca.setTime(new Date()); 
		ca.add(Calendar.DAY_OF_MONTH, -7); 
		Date start_time = ca.getTime(); //结果
		
//		FailureInfoPhoenixService failureInfoPhoenixService = (FailureInfoPhoenixService)ContextAccessor.getBean("failureInfoPhoenixService");
//		List<Map<String, Object>> devices = failureInfoPhoenixService.queryFailureFrequence(sdf.format(start_time), city_code);		
		FailureInfoHistoryService failureInfoHistoryService = (FailureInfoHistoryService)ContextAccessor.getBean("failureInfoHistoryService");
		List<Map<String, Object>> devices = failureInfoHistoryService.queryFailureRank(sdf.format(start_time), city_code);	
		
		JSONArray frequence_rank = new JSONArray();
		
		for(Map<String, Object> device : devices){
			String station_id = (String) device.get("station_code");
			String station_name = (String) device.get("station_name");
			String device_id = (String) device.get("device_id");
			long failure_times = (Long) device.get("failure_times");
			JSONObject device_details = new JSONObject();
			device_details.put("station_id", station_id);
			device_details.put("station_name", station_name);
			device_details.put("device_id",device_id );
			device_details.put("failure_times", failure_times);
			frequence_rank.add(device_details);
			if(frequence_rank.size()==5)   //只取前5
				break;
		}
		return frequence_rank;
	}

}
