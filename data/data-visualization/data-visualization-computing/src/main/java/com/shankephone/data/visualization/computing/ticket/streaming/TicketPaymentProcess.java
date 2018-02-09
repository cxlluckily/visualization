package com.shankephone.data.visualization.computing.ticket.streaming;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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
import com.shankephone.data.visualization.computing.ticket.offline.TicketPaymentAnalyse;

/**
 * 实时分析支付
 * @author fengql
 * @version 2017年9月20日 上午9:40:41
 */
public class TicketPaymentProcess implements Executable{
	
	private final static Logger logger = LoggerFactory.getLogger(TicketPaymentProcess.class);
	
	public static void main(String[] args) {
		TicketPaymentProcess p = new TicketPaymentProcess();
		Map<String,String> map = new HashMap<String,String>();
		Date date = new Date();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date); 
		cal.add(Calendar.SECOND, -5);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		map.put("startTime", sdf.format(cal.getTime()));
		p.execute(map); 
	}
	
	@Override
	public void execute(Map<String, String> argsMap) {
		/*String timestamp = argsMap == null ? null : argsMap.get("startTime");
		String time = IAnalyseHandler.getLongTimestamp(timestamp);
		analysePayment(time);*/
		
		String enable = argsMap == null ? null : argsMap.get("enableOffline");
		if(enable != null && !"".equals(enable) && "true".equals(enable)){
			executeAll(argsMap);
		} else {
			executeStreaming(argsMap);
		}
	}
	
	/**
	 * 离线任务调用 
	 * @param argsMap
	 */
	public void executeOffline(Map<String, String> argsMap){
		TicketPaymentAnalyse offlineAnalyse = new TicketPaymentAnalyse();
		String timestamp = argsMap == null ? null : argsMap.get("endTime");
		offlineAnalyse.analysePayment("ticket_payment", timestamp);
	}
	
	/**
	 * 流处理任务调用 
	 * @param argsMap
	 */
	public void executeStreaming(Map<String, String> argsMap) {
		String timestamp = argsMap == null ? null : argsMap.get("startTime");
		String time = IAnalyseHandler.getLongTimestamp(timestamp);
		analysePayment(time);
	}
	
	/**
	 * 启用离线+实时分析
	 * @param argsMap
	 */
	public void executeAll(Map<String, String> argsMap){
		String timestamp = argsMap == null ? null : argsMap.get("timestamp");
		TicketPaymentAnalyse offlineAnalyse = new TicketPaymentAnalyse();
		offlineAnalyse.analysePayment("ticket_payment", timestamp);
		String time = IAnalyseHandler.getLongTimestamp(timestamp);
		analysePayment(time);
	}
	
	/**
	 * 支付渠道分析
	 * @author fengql
	 * @date 2017年9月12日 下午6:02:05
	 * @param t_last_timestamp
	 * @param enable 是否使用时间戳限制最后修改时间，实时处理指定时间点时为true
	 */
	public void analysePayment(String t_last_timestamp){
		try {
				SparkStreamingBuilder builder = new SparkStreamingBuilder("paymentChannel");
				JavaStreamingContext streamingContext = builder.getStreamingContext();
				JavaInputDStream<ConsumerRecord<String, String>> stream;
				long timestamp ;
				if (t_last_timestamp==null){
					timestamp =0L;
					stream = builder.createKafkaStream();
				}
				else{
					timestamp = Long.parseLong(t_last_timestamp);
					stream = builder.createKafkaStream(timestamp);
				}
				stream.foreachRDD(rdd -> {
					OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
					JavaPairRDD<Map<String, String>, Integer> pairRdd = rdd.filter(f->{
						JSONObject value = JSONObject.parseObject(f.value());
						return PayTypeUtils.filterTicketDataPayType(value, timestamp);
					}).flatMapToPair(f->{
						JSONObject value = JSONObject.parseObject(f.value());
						return PayTypeUtils.mapToUsefulForPayType(value);
					}).reduceByKey((a,b)->a+b);
					
					List<Tuple2<Map<String, String>,Integer>> list = pairRdd.collect();
					
					RedissonClient redisson =  RedisUtils.getRedissonClient();
					for(Tuple2<Map<String, String>, Integer> record : list){
						 Map<String, String> key = record._1;
						 String cityCode = key.get("cityCode");
						 String date = key.get("date");
						 if(cityCode == null || "".equals(cityCode)){
							 continue;
						 }
						 if(date == null || "".equals(date)){
							 continue;
						 }
						 PayTypeUtils.countTicketsPayType(key.get("type"), cityCode, date, record._2, redisson);
					 }
					((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
				});
				streamingContext.start();
				logger.info("--------ticket payment realtime task is executing....--------");
				streamingContext.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	

}
