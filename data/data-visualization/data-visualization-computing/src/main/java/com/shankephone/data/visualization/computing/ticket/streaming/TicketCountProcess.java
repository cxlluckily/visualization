package com.shankephone.data.visualization.computing.ticket.streaming;

import java.util.ArrayList;
import java.util.HashMap;
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

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingBuilder;
import com.shankephone.data.common.spark.SparkStreamingBuilder.KafkaOffset;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;

import scala.Tuple2;


/**
 * 售票量 与 热门站点 统计
 * @author 森
 * @version 2017年9月15日 下午2:02:35
 */
public class TicketCountProcess implements Executable {
	private final static Logger logger = LoggerFactory.getLogger(TicketCountProcess.class);
	@Override
	public void execute(Map<String, String> argsMap) {
		
		SparkStreamingBuilder builder = new SparkStreamingBuilder("order-ticketCount");
		JavaInputDStream<ConsumerRecord<String, String>> stream;
		long t_last_timestamp;
		if (argsMap==null||argsMap.get("startTime")==null){
			stream = builder.createKafkaStream();
			t_last_timestamp = 0l;
		}
		else{
			t_last_timestamp = Long.parseLong(IAnalyseHandler.getLongTimestamp(argsMap.get("startTime")));
			stream = builder.createKafkaStream(t_last_timestamp);
		}
		
		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

			//计算
			JavaPairRDD<Map<String, String>, Integer> results = rdd.filter(f->{
				JSONObject value = JSONObject.parseObject(f.value());
				return TicketUtils.filterTicketData(value, t_last_timestamp);
			}).flatMapToPair(f->{
				JSONObject value = JSONObject.parseObject(f.value());
				return TicketUtils.mapToUseful(value);
			}).reduceByKey((a,b)->a+b);
			//输出

			 List<Tuple2<Map<String, String>,Integer>> records = results.collect();
			 RedissonClient redisson =  RedisUtils.getRedissonClient();
			 List<Tuple2<Map<String, String>,Map<String,Integer>>> listByMap=new ArrayList<Tuple2<Map<String, String>,Map<String,Integer>>>();
			 if(records!=null&&records.size()>0){
				 listByMap=setList(listByMap,records);
				 for(Tuple2<Map<String, String>, Map<String,Integer>> record : listByMap){
					 Map<String, String> key = record._1;
					 TicketUtils.countTickets(key.get("cityCode"), key.get("date"), record._2, redisson);
				 }
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
	//重构数据结构
	public List<Tuple2<Map<String, String>,Map<String,Integer>>> setList(List<Tuple2<Map<String, String>,Map<String,Integer>>> listByMap,List<Tuple2<Map<String, String>,Integer>> records){
		List<Tuple2<Map<String, String>,Integer>> records_=new ArrayList<Tuple2<Map<String,String>,Integer>>(records);
		List<Tuple2<Map<String, String>,Integer>> records_new=new ArrayList<Tuple2<Map<String,String>,Integer>>();
		
		Tuple2<Map<String, String>, Integer> record=null;
		String dateFlag="";
		String cityCodeFlag="";
		Map<String, String> sortMap = new HashMap<String, String>();
		Map<String, Integer> detailMap = new HashMap<String, Integer>();
		if(records_!=null&&records_.size()>0){
			for(int i=0;i<records_.size();i++){
				record=records_.get(i);
				Map<String, String> key = record._1;
				if(i==0){
					dateFlag=key.get("date");
					cityCodeFlag=key.get("cityCode");
					sortMap.put("date", dateFlag);
					sortMap.put("cityCode", cityCodeFlag);
					detailMap.put(key.get("type"), record._2);
					records_new.add(record);
				}else if(dateFlag.equals(key.get("date"))&&cityCodeFlag.equals(key.get("cityCode"))){
					detailMap.put(key.get("type"), record._2);
					records_new.add(record);
				}
			}
			listByMap.add(new Tuple2<Map<String, String>,Map<String,Integer>>(sortMap,detailMap));
		}
		records_.removeAll(records_new);
		if(records_!=null && records_.size()>0){
			setList(listByMap,records_);
		}
		return listByMap;
	}
}
