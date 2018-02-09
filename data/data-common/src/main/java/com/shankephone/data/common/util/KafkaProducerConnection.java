package com.shankephone.data.common.util;

import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class KafkaProducerConnection implements Serializable {

	private static final long serialVersionUID = 1L;
	private static KafkaProducerConnection instance = null;
	private Producer<String, String> producer = null;
	
	
	public KafkaProducerConnection() {
		createKafkaProducer();
	}

	public static KafkaProducerConnection getInstance(){
		if ( instance==null ){
			instance = new KafkaProducerConnection();
			System.out.println("初始化 kafka producer...");
		}
		return instance;
	}
	
	private void createKafkaProducer() {
		Properties props = PropertyAccessor.getProperties("kafkaproducer");
		props.setProperty("client.id", props.getProperty("client.id")+"_"+UUID.randomUUID());
		producer = new KafkaProducer<String, String>(props);
	}

	public Producer<String, String> getProducer() {
		return producer;
	}
	
	
}
