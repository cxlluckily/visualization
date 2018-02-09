package com.shankephone.report.computing;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.junit.Test;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;

public class TestSparkComputingDirectStream {
	private static String topic = "mysql-data";
	private static String appName = "Spark-Computing-Test";
	private static String master = "local[*]";
	private static String brokers = "data1.shankephone.com:6667,data2.shankephone.com:6667,data3.shankephone.com:6667";
	private static long duration = 1000;
	private static String groupId = "computing-grp1";

	private static JavaStreamingContext streamingContext;
	private static SparkConf conf;
	
	@Test
	public void test1Consume(){
		SparkConf conf = getSparkConf();
		JavaStreamingContext context = getJavaStreamingContext(conf);
		consume1(context);
	}
	
	public static void consume1(JavaStreamingContext streamingContext) {
		Collection<String> topics = Arrays.asList(topic);
		Map<String, Object> kafkaParams = getKafkParams();
		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
				.createDirectStream(streamingContext, LocationStrategies
						.PreferConsistent(), ConsumerStrategies
						.<String, String> Subscribe(topics, kafkaParams));
		DoubleAccumulator accum = streamingContext.sparkContext()
				.sc().doubleAccumulator();
		LongAccumulator count = streamingContext.sparkContext()
				.sc().longAccumulator();
		stream.mapToPair(r -> {
			String key = r.key();
			String value = r.value();
			JSONObject keyJson = JSONObject.parseObject(key);
			JSONObject valueJson = JSONObject.parseObject(value);
			JSONObject jsonKey = keyJson.getJSONObject("keys");
			String orderId = "0";
			if(jsonKey != null){
				orderId = keyJson.getJSONObject("keys").getString("ORDER_ID");
				if(orderId == null){
					orderId = "0";
				}
			}
			
			/*StringBuffer ids = new StringBuffer("").append(keyJson.getString("serverId"))
					.append("#@" + keyJson.getString("schemaName"))
					.append("#@" + keyJson.getString("tableName"))
					.append("#@" + orderId);*/
			StringBuffer ids = new StringBuffer("").append(orderId);
			JSONObject column = valueJson.getJSONObject("columns");
			String vs = "0";
			if(column != null){
				vs = column.getString("TOTAL_AMOUNT");
				vs = vs == null ? "0" : vs;
			}
			Double val = Double.parseDouble(vs);
			//System.out.println("=======================" + ids + "=" + vs + "========================");
			return new Tuple2<>(ids.toString(), val);
		})
		.map(r -> {
			return r._2;
		}).reduce((o,n)-> {
			count.add(1);
			System.out.println("-----------" + count.value());
			//System.out.println(o + " + " + n + " = " + (o + n));
			return o + n;
		}).foreachRDD(r -> {
			Iterator<Double> it = r.collect().iterator();
			while(it.hasNext()){
				accum.add(it.next());
			}
			System.out.println("========" + count.value() + "========" + accum.value()); 
		});
		streamingContext.start(); // Start the computation
		System.out.println("---------------------start---------------------");
		try {
			streamingContext.awaitTermination(); // Wait for the computation to terminate
			// ((CanCommitOffsets)
			// stream.inputDStream()).commitAsync(offsetRanges);
			// streamingContext.stop(false);
			streamingContext.stop();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	

	public static Map<String, Object> getKafkParams() {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		kafkaParams.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		kafkaParams.put("group.id", groupId);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("max.poll.records",500);
		kafkaParams.put("enable.auto.commit", false);
		return kafkaParams;
	}

	public static JavaStreamingContext getJavaStreamingContext(SparkConf conf) {
		if (streamingContext != null) {
			return streamingContext;
		}
		streamingContext = new JavaStreamingContext(conf,
				new Duration(duration));
		return streamingContext;
	}

	public static SparkConf getSparkConf() {
		if (conf != null) {
			return conf;
		}
		conf = new SparkConf().setAppName(appName).setMaster(master);
		return conf;
	}

}