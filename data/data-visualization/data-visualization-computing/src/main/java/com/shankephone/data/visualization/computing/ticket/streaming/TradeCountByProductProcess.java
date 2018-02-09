/**
 * 
 */
package com.shankephone.data.visualization.computing.ticket.streaming;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingBuilder;
import com.shankephone.data.common.spark.SparkStreamingBuilder.KafkaOffset;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;

/**      
 * @author yaoshijie  
 * @date 2018年2月1日  
 * @Description: TODO  
 */
public class TradeCountByProductProcess implements Executable {
	
	private final static Logger logger = LoggerFactory.getLogger(TradeCountByProductProcess.class);
	
	@Override
	public void execute(Map<String, String> argsMap) {
		String timestamp = argsMap == null ? null : argsMap.get("startTime");
		String time = IAnalyseHandler.getLongTimestamp(timestamp);
		analyseTrade(time);
	}

	/**
	 * 
	* @Title: analyseTrade 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param t_last_timestamp    参数  
	* @return void    返回类型  
	* @throws
	 */
	public void analyseTrade(String t_last_timestamp){
		SparkStreamingBuilder builder = new SparkStreamingBuilder("order-ticketSourceCount");
		JavaInputDStream<ConsumerRecord<String, String>> stream;
		long timestamp_long ;
		if (t_last_timestamp==null){
			timestamp_long =0L;
			stream = builder.createKafkaStream();
		}
		else{
			timestamp_long = Long.parseLong(t_last_timestamp);
			stream = builder.createKafkaStream(timestamp_long);
		}
		stream.foreachRDD(rdd ->{
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			//计算
			JavaPairRDD<Map<String, String>, Integer> results = rdd.filter(f->{
				JSONObject value = JSONObject.parseObject(f.value());
				return TradeUtils.filterTicketData(value, timestamp_long);
			}).flatMapToPair(f->{
				JSONObject value = JSONObject.parseObject(f.value());
				return TradeUtils.mapToUseful(value);
			}).reduceByKey((a,b)->a+b);
			
			List<Tuple2<Map<String, String>,Integer>> records = results.collect();
			 RedissonClient redisson =  RedisUtils.getRedissonClient();
			 
			 for(Tuple2<Map<String, String>, Integer> record : records){

				 Map<String, String> key = record._1;
				 TradeUtils.countTickets(key.get("type"), key.get("cityCode"), key.get("date"), record._2, redisson);
			 }
			((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
		});
		
		try {
			builder.getStreamingContext().start();
			builder.getStreamingContext().awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
