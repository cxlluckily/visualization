package com.shankephone.data.common.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamingToKafkaConnection {
	/**
	 * @todo sparkstreaming的InputDStream
	 */
	private JavaInputDStream<ConsumerRecord<String, String>> stream = null;
	/**
	 * @todo sparkstreaming的context
	 */
	private JavaStreamingContext context = null;
	/**
	 * @todo 机制对象
	 */
	private  Logger logger = LoggerFactory.getLogger(SparkStreamingToKafkaConnection.class);
	
	
	/**
	 * @param AppName
	 * @param Master
	 * @param Duration
	 */
	private SparkStreamingToKafkaConnection(String AppName,String Master,int Duration,String kafkaConfPreName,String kafkaTopicConfName) {
		SparkStreamingToKafkaConnect(AppName,Master,Duration,kafkaConfPreName,kafkaTopicConfName);
	}
	
	/** 
	 * @todo 获取连接实例
	 * @author sunweihong
	 * @date 2017年6月8日 下午2:30:32
	 * @param AppName 应用名称（yarn显示）
	 * @param Master sparkstreaming的master
	 * @param Duration 设置批量处理的时间间隔
	 * @param kafkaConfPreName kafka配置文件中的前缀名
	 * @param kafkaTopicConfName kafka配置文件中的访问topic的名称
	 * @return
	 */
	public static SparkStreamingToKafkaConnection getInstance(String AppName,String Master,int Duration,String kafkaConfPreName,String kafkaTopicConfName){
		return new SparkStreamingToKafkaConnection( AppName, Master, Duration,kafkaConfPreName,kafkaTopicConfName);
	}

	/** 
	 * @todo sparkstreaming获取kafka的连接
	 * @author sunweihong
	 * @date 2017年6月8日 下午2:20:21
	 * @param AppName
	 * @param Master
	 * @param Duration
	 */
	private void  SparkStreamingToKafkaConnect(String AppName,String Master,int Duration,String kafkaConfPreName,String kafkaTopicConfName){
		try {
			SparkConf conf = new SparkConf().setAppName(AppName).setMaster(Master);
			context = new JavaStreamingContext(conf, new Duration(Duration));
			
			Map<String, Object> kafkaParams = new HashMap<>();
			Properties props = PropertyAccessor.getProperties(kafkaConfPreName);
			Set<Object> keySet = props.keySet();
			for(Object obj : keySet){
				String key = (String)obj;
				String value = props.getProperty(key);
				kafkaParams.put(key,value);
			}
			Collection<String> topics = Arrays.asList(PropertyAccessor.getProperty(kafkaTopicConfName));

			stream = KafkaUtils.createDirectStream( 
					context, 
					LocationStrategies.PreferConsistent(),  
					ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
			);
			logger.info("【spark-streaming】 spark-streaming 连接 kafka 集群"+PropertyAccessor.getProperty(kafkaTopicConfName)+"，成功！");
		} catch (Exception e) {
			logger.info("【spark-streaming】 spark-streaming 连接 kafka 集群"+PropertyAccessor.getProperty(kafkaTopicConfName)+"，失败！");
			e.printStackTrace();
		}
		
	}

	public JavaInputDStream<ConsumerRecord<String, String>> getStream() {
		return stream;
	}


	public JavaStreamingContext getContext() {
		return context;
	}

	
}
