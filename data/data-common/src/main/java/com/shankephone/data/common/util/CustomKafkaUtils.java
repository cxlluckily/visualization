package com.shankephone.data.common.util;

import java.util.Arrays;
import java.util.Properties;

import kafka.consumer.Consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomKafkaUtils {
	
	public static KafkaConsumer<String, String> createKafkaConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "data3.test:6667,data5.test:6667,data2.test:6667,data1.test:6667,data4.test:6667");
		props.put("group.id", "computing-amount");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("storage"));
		return consumer;
	}
}
