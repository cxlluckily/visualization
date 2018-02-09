package com.shankephone.data.storage.convertor;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.storage.exception.SyncException;

public class CityConvertor implements Convertible{

	private static Map<String,String> cityMap = new HashMap<String,String>();
	
	static {
		cityMap.put("510000001","4401");
		cityMap.put("510000002","4401");
		cityMap.put("510000002_2002","4401");
		cityMap.put("510000003","4401");
		cityMap.put("default","4401");
	}
	
	@Override
	public Map<String,String> getValue(String name, JSONObject json){
		Map<String,String> map = new HashMap<String,String>();
		if(json == null || "".equals(json)){
			throw new SyncException("自定义规则处理：[" + json + "]为空！");
		}
		String cityCode = json.getString(name);
		String code = "";
		Pattern p = Pattern.compile("\\d+");
		Matcher m = p.matcher(cityCode);
		if(m.find()){
			code = m.group(0);
			String value = cityMap.get(code);
			if(value == null){
				value = code.substring(0,4);
			}
			map.put(name, value);
		} else {
			String value = cityMap.get("default");
			map.put(name, value);
		}
		return map;
	}
	
	
	public static void main(String[] args) {
		CityConvertor convertor = new CityConvertor();
		JSONObject json = new JSONObject();
		json.put("schemaName", "510000002_2005");
		JSONObject ch = new JSONObject();
		ch.put("partner_id", "510000002_2005");
		json.put("columns", ch);
		
		System.out.println(json.toJSONString());
		Map<String,String> m = convertor.getValue("schemaName", json);
		System.out.println(m);
		Map<String,String> m1 = convertor.getValue("partner_id", json.getJSONObject("columns"));
		System.out.println(m1);
	}


}
