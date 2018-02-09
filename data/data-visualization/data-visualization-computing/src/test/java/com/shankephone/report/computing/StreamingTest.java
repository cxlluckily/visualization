package com.shankephone.report.computing;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class StreamingTest {
	public static void main(String[] args) throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "data1.shankephone.com:6667,data2.shankephone.com:6667,data3.shankephone.com:6667");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);

		Collection<String> topics = Arrays.asList("mysql-data");
		
		SparkConf conf = new SparkConf().setMaster("yarn").setAppName("StreamingTest");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
			jssc,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );

		stream.foreachRDD(rdd -> {
			rdd.foreach(record -> {
				System.out.println(record.value());
				HttpClient client = new DefaultHttpClient();
				HttpGet request = new HttpGet("http://192.168.5.226/?"+ record.offset());
				HttpResponse response = client.execute(request);
				request.releaseConnection();
				client.getConnectionManager().shutdown();
			});
		});
		jssc.start();
		jssc.awaitTermination();
		//jssc.stop();
		//jssc.close();
	}
}