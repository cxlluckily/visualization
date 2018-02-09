package com.shankephone.data.common.spark;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import com.alibaba.fastjson.JSONObject;

public interface SparkStreamingProcessor {
	
	public void process(JavaRDD<JSONObject> rdd, Map<String, String> args);
	
}
