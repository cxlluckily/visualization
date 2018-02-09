
package com.shankephone.data.visualization.computing.user.realtime;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.kafka.KafkaHelper;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.util.SparkStreamingToKafkaConnection;


public class StatisticsUserStreamingProcess {
	
	private StatisticsUserStreamingProcess(){
		
	}
	
	public static StatisticsUserStreamingProcess getInstance(){
		return new StatisticsUserStreamingProcess();
	}
	
	/** 
	 * @author hong
	 * @date 2017年6月1日 上午9:40:56
	 * @throws Exception
	 */
	public void  startStatistics(long t_last_timestamp){
		try {
				Map<String, Object> kafkaParams = new HashMap<>();
				Properties props = PropertyAccessor.getProperties("kafkaconf2");
				Set<Object> keySet = props.keySet();
				for(Object obj : keySet){
					String key = (String)obj;
					String value = props.getProperty(key);
					kafkaParams.put(key,value);
				}
				KafkaHelper.setOffset(kafkaParams, PropertyAccessor.getProperty("kafka.topics"), t_last_timestamp);
				SparkStreamingToKafkaConnection sstkc = SparkStreamingToKafkaConnection.getInstance(
						PropertyAccessor.getProperty("user.spark.app.name"), 
						PropertyAccessor.getProperty("spark.app.master"), 
						Integer.parseInt(PropertyAccessor.getProperty("spark.app.duration")),
						"kafkaconf2",
						"kafka.topics");
				JavaStreamingContext streamingContext = sstkc.getContext();
				JavaInputDStream<ConsumerRecord<String, String>> stream = sstkc.getStream();
				stream.foreachRDD(rdd -> {
					OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
					rdd.foreachPartition(f->{
						try {
							StatisticsUserService tsm = StatisticsUserService.getInstance();
							tsm.settLastTimestamp(t_last_timestamp);
							while(f.hasNext()){
								ConsumerRecord<String,String> next = f.next();
								JSONObject value = JSONObject.parseObject(next.value());
								if(tsm.filterUserUsefulData(value)){
									tsm.startStatisticsUserCount(value);							
									
								}
								
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
					((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
				});
				streamingContext.start();
				streamingContext.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}

		
	}

	
	
	
	
	

}
