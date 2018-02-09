package com.shankephone.data.common.spark;

import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.spark.SparkStreamingBuilder.KafkaOffset;
import com.shankephone.data.common.spark.config.StreamingApp;
import com.shankephone.data.common.spark.config.StreamingConfig;
import com.shankephone.data.common.util.XmlUtils;

public class SparkStreamingExecutor {
	private final static Logger logger = LoggerFactory.getLogger(SparkStreamingExecutor.class);
	
	public void execute(Map<String, String> args) {
		String appId = args.get("appId");
		String startTimestamp = args.get("startTime");
		if (StringUtils.isBlank(appId)) {
			throw new RuntimeException("appId不能为空");
		}
		StreamingConfig config = XmlUtils.xml2Java("streamingConfig.xml", StreamingConfig.class);
		StreamingApp app = config.getApp(appId);
		if (app == null) {
			throw new RuntimeException("streamingConfig.xml中不存在Id为：" + appId + "的app。");
		}
		List<String> processors = app.getProcessors();
		if (processors.isEmpty()) {
			throw new RuntimeException("streamingConfig.xml中Id为：" + appId + "的app，不存在任何processor");
		}
		SparkStreamingBuilder builder = new SparkStreamingBuilder(appId);
		JavaInputDStream<ConsumerRecord<String, String>> ds = createKafkaStream(builder, startTimestamp);
		ds.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			JavaRDD<JSONObject> mapRdd = transformKafkaValue(rdd);
			JavaRDD<JSONObject> filteredRdd = filterRdd(mapRdd, startTimestamp);
			JavaRDD<JSONObject> cachedRdd = filteredRdd.cache();
			for (String processor : processors) {
				Class<SparkStreamingProcessor> processorClass = (Class<SparkStreamingProcessor>) Class.forName(processor);
				SparkStreamingProcessor processorInstance = processorClass.newInstance();
				processorInstance.process(cachedRdd, args);
			}
			((CanCommitOffsets) ds.inputDStream()).commitAsync(offsetRanges);
		});
		builder.getStreamingContext().start();
		try {
			builder.getStreamingContext().awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public JavaRDD<JSONObject> transformKafkaValue(JavaRDD<ConsumerRecord<String, String>> rdd) {
		JavaRDD<JSONObject> resultRdd = rdd.map(f -> {
			String kafkaValue = f.value();
			return JSONObject.parseObject(kafkaValue);
		});
		return resultRdd;
	}
	
	public JavaRDD<JSONObject> filterRdd(JavaRDD<JSONObject> rdd, String startTimestamp) {
		if (StringUtils.isBlank(startTimestamp) 
				|| startTimestamp.equalsIgnoreCase("start") 
				|| startTimestamp.equalsIgnoreCase("end")) {
			return rdd;
		}
		JavaRDD<JSONObject> resultRdd = rdd.filter(f -> {
			String lastTimestamp = f.getString("T_LAST_TIMESTAMP");
			return Long.parseLong(lastTimestamp) > Long.parseLong(startTimestamp);
		});
		return resultRdd;
	}
	
	public JavaInputDStream<ConsumerRecord<String, String>> createKafkaStream(SparkStreamingBuilder builder, String startTimestamp) {
		if (StringUtils.isBlank(startTimestamp)) {
			return builder.createKafkaStream();
		}
		if (startTimestamp.equalsIgnoreCase("start")) {
			return builder.createKafkaStream(KafkaOffset.START);
		}
		if (startTimestamp.equalsIgnoreCase("end")) {
			return builder.createKafkaStream(KafkaOffset.END);
		}
		return builder.createKafkaStream(Long.parseLong(startTimestamp));
	}
	
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		Option appIdO = Option.builder("appId")
											.required()
											.hasArg()
											.build();
		Option startTimeO = Option.builder("startTime")
											.hasArg()
											.build();
		Option argumentO = Option.builder("args")
										   .hasArg()
										   .numberOfArgs(2)
										   .valueSeparator()
										   .build();
		options.addOption(appIdO);
		options.addOption(startTimeO);
		options.addOption(argumentO);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		
		String appId = cmd.getOptionValue("appId");
		String startTime = cmd.getOptionValue("startTime");
		Map<String, String> arguments = (Map)cmd.getOptionProperties("args");
		arguments.put("appId", appId);
		if (startTime != null) {
			arguments.put("startTime", startTime);
		}
		logger.info("args : [" + arguments + "]");
		new SparkStreamingExecutor().execute(arguments);
	}
	
}
