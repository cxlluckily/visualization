package com.shankephone.data.visualization.computing.ticket.streaming;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.redisson.api.RBucket;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.cache.City;
import com.shankephone.data.cache.CityCache;
import com.shankephone.data.cache.Station;
import com.shankephone.data.cache.StationCache;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkStreamingBuilder;
import com.shankephone.data.common.spark.SparkStreamingProcessor;
import com.shankephone.data.common.spark.SparkStreamingBuilder.KafkaOffset;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;
import com.shankephone.data.visualization.computing.common.util.Constants;

/**
 * 实时交易记录处理
 * @author fengql
 * @version 2018年2月5日 下午2:53:13
 */
public class TicketRealtimeProcessor implements SparkStreamingProcessor {
	
	private final static Logger logger = LoggerFactory.getLogger(TicketRealtimeProcessor.class);
	
	private static FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

	public void process(JavaRDD<JSONObject> rdd, Map<String, String> args) {
		rdd.filter(f->{
			return filter(f);
		}).foreach(f->{
			process(f);
		});
	}	
	
	/**
	 * 数据过滤
	 * @param value
	 * @param timestamp
	 * @return
	 */
	public static boolean filter(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		if(columns == null || columns.size() == 0){
			return false;
		}
		if(tableName == null || columns == null) {
			return false;
		}
		String cityCode = columns.getString("CITY_CODE");
		if(cityCode == null || "".equals(cityCode)){
			return false;
		}
		if (StringUtils.isNotBlank(cityCode)) {
			RedissonClient redisson = RedisUtils.getRedissonClient();
			if (value.containsKey("changes") && 
					("SKP:ORDER_INFO".equals(tableName) 
							|| "COFFEE:T_TASTE_ORDER".equals(tableName))) {
				boolean statusFlag = false;
				boolean preStatusFlag = false;
				boolean ticketFlag = false;
				boolean dateFlag = false;
				JSONObject changes = value.getJSONObject("changes");
				if ("SKP:ORDER_INFO".equals(tableName)) {
					String productCode = columns.getString("ORDER_PRODUCT_CODE");
					String productCodePre = "";
					if ( StringUtils.isNotBlank(productCode)) {
						productCodePre = productCode.split("_")[0];
						if ("DCP".equals(productCodePre) || "TVIP".equals(productCodePre) || "GZH".equals(productCodePre)) { 				//单程票
							String entryStationCode = columns.getString("TICKET_PICKUP_STATION_CODE");
							String exitStationCode = columns.getString("TICKET_GETOFF_STATION_CODE");
							String ticketNum = columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
							String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
							statusFlag = "5".equals(columns.getString("TICKET_ORDER_STATUS"));
							preStatusFlag = !"5".equals(changes.getString("TICKET_ORDER_STATUS"));
							ticketFlag = StringUtils.isNotBlank(entryStationCode) && StringUtils.isNotBlank(ticketNum);
							dateFlag = StringUtils.isNotBlank(time);
							
							if (statusFlag && preStatusFlag && ticketFlag && dateFlag) {
								JSONObject publishData = new JSONObject();
								Station entryStation = StationCache.getInstance().get(cityCode, entryStationCode);
								Station exitStation = StationCache.getInstance().get(cityCode, exitStationCode);
								publishData.put("cityName", Constants.cityMap.get(cityCode));
								if (entryStation == null) {
									publishData.put("entryStation", null);
								} else {
									publishData.put("entryStation", entryStation.getStationNameZh());
								}
								if (exitStation == null) {
									publishData.put("exitStation", null);
								} else {
										publishData.put("exitStation", exitStation.getStationNameZh());
								}
								publishData.put("ticketType", Constants.orderTypeMap.get(Constants.ORDER_TYPE_DCP));
								publishData.put("ticketNum", Integer.valueOf(ticketNum));
								publishData.put("time", time.split(" ")[0]);
							}
							
						} else if ("CSDT".equals(productCode) || "NFC_SIM".equals(productCode) || "CSSH_A".equals(productCode) || "CSSH_I".equals(productCode)) {	//  长沙充值
							statusFlag = "5".equals(columns.getString("TOPUP_ORDER_STATUS"));
							preStatusFlag = !"5".equals(changes.getString("TOPUP_ORDER_STATUS"));
							ticketFlag = true;
							dateFlag = StringUtils.isNotBlank(columns.getString("TOPUP_TOPUP_DATE"));
							
						} else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//珠海有轨（没有站点信息）
							statusFlag = "5".equals(columns.getString("TICKET_RDER_STATUS"));
							preStatusFlag = !"5".equals(changes.getString("TICKET_ORDER_STATUS"));
							ticketFlag = StringUtils.isNotBlank(columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM"));
							dateFlag = StringUtils.isNotBlank(columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE"));
						}
					} 	
				} else {			//咖啡  
					statusFlag = "4".equals(columns.getString("ORDER_STATE"));
					preStatusFlag = !"4".equals(changes.getString("ORDER_STATE"));
					ticketFlag = true;
					dateFlag = StringUtils.isNotBlank(columns.getString("UPDATE_DATE"));
				}
				return (statusFlag && preStatusFlag && ticketFlag && dateFlag);
			} else {
				if ("SKP:SJT_QR_CODE".equals(tableName)) { 	//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
					String saleDate = columns.getString("SJT_SALE_DATE");
					String entryStationCode = columns.getString("SJT_ENTRY_STATION_CODE");
					String exitStationCode = columns.getString("SJT_EXIT_STATION_CODE");
					if (!("03".equals(columns.getString("SJT_STATUS")) || "99".equals(columns.getString("SJT_STATUS")))
									&& StringUtils.isNotBlank(columns.getString("ORDER_NO")) && StringUtils.isNotBlank(entryStationCode)
									&& StringUtils.isNotBlank(saleDate)) {
						
						saleDate = saleDate.split(" ")[0];
						RSet<String> set = redisson.getSet("order:uniq:" + "xfhx:" + cityCode + ":" + saleDate);
						if (set.add(columns.getString("ORDER_NO"))) {
							JSONObject publishData = new JSONObject();
							publishData.put("cityName", Constants.cityMap.get(cityCode));
							Station entryStation = StationCache.getInstance().get(cityCode, entryStationCode);
							Station exitStation = StationCache.getInstance().get(cityCode, exitStationCode);
							if (entryStation == null) {
								publishData.put("entryStation", null);
							} else {
								publishData.put("entryStation", entryStation.getStationNameZh());
							}
							if (exitStation == null) {
								publishData.put("exitStation", null);
							} else {
									publishData.put("exitStation", exitStation.getStationNameZh());
							}
							publishData.put("ticketType", Constants.orderTypeMap.get(Constants.ORDER_TYPE_XFHX));
							publishData.put("ticketNum", 1);
							publishData.put("time", saleDate);
							return true;
						}
					} else {
						return false;
					}
					
				} else if ("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)) {		//先享后付  SKP:METRO_MEMBER_SUBSCRIPTION_TRANS （没有上下站点信息字段  record里有   表连接需求）
					FastDateFormat fastDateFormat2 = FastDateFormat.getInstance("yyyyMMdd");
					String seq = columns.getString("TIKCET_TRANS_SEQ");
					String cardNum = columns.getString("METRO_MEMBER_CARD_NUM");
					String transDate = columns.getString("TRANS_DATE");
					if (StringUtils.isNotBlank(seq) && StringUtils.isNotBlank(cardNum) && StringUtils.isNotBlank(transDate)) {
						try {
							transDate = fastDateFormat.format(fastDateFormat2.parse(columns.getString("TRANS_DATE")));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						
						String entryStationCode = columns.getString("ENTRY_STATION_CODE");
						String exitStationCode = columns.getString("TRANS_STATION_CODE");
						if ("05".equals(columns.getString("TICKET_STATUS")) && value.containsKey("changes") 
									&&!"05".equals(value.getJSONObject("changes").getString("TICKET_STATUS")) 
									&& StringUtils.isNotBlank(entryStationCode) && StringUtils.isNotBlank(exitStationCode)) {   
							JSONObject publishData = new JSONObject();
							publishData.put("cityName", Constants.cityMap.get(cityCode));
							Station entryStation = StationCache.getInstance().get(cityCode, entryStationCode);
							Station exitStation = StationCache.getInstance().get(cityCode, exitStationCode);
							if (entryStation == null) {
								publishData.put("entryStation", null);
							} else {
								publishData.put("entryStation", entryStation.getStationNameZh());
							}
							if (exitStation == null) {
								publishData.put("exitStation", null);
							} else {
									publishData.put("exitStation", exitStation.getStationNameZh());
							}
							publishData.put("ticketType", Constants.orderTypeMap.get(Constants.ORDER_TYPE_XXHF));
							publishData.put("ticketNum", 1);
							publishData.put("time", transDate);
						}
						return true;
					}
				}
			}
		}
		return false;
	
	}
	
	/**
	 * 实时处理与推送
	 * @param value
	 */
	private static void process(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		String cityCode = columns.getString("CITY_CODE");
		//先享后付交易类型，用于显示进出站标识
		String trx_type = columns.getString("TRX_TYPE");
		if(cityCode == null || "".equals(cityCode)){
			logger.error("城市代码为空！" + value);
			return;
		}
		String type = null;
		Integer ticketNum = 0;
		String datetime = null;
		//进站、出站
		String entryStation = null;
		String exitStation = null;
		JSONObject json = new JSONObject();
		if ("SKP:ORDER_INFO".equals(tableName) ) { //单程票和珠海先付后享
			entryStation = columns.getString("TICKET_PICKUP_STATION_CODE");
			exitStation = columns.getString("TICKET_GETOFF_STATION_CODE");
			String productCode = columns.getString("ORDER_PRODUCT_CODE");
			String productCodePre = productCode.split("_")[0];
			if ("DCP".equals(productCodePre) || "TVIP".equals(productCodePre) || "GZH".equals(productCodePre)) { 				//单程票
				type = Constants.ORDER_TYPE_DCP;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				datetime = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			} else if ("CSDT".equals(productCode) || "NFC_SIM".equals(productCode) || "CSSH_A".equals(productCode) || "CSSH_I".equals(productCode)) {	//  长沙充值
				type = Constants.ORDER_TYPE_NFC;
				ticketNum = 1;
				datetime = columns.getString("TOPUP_TOPUP_DATE");
			} else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//珠海有轨（无站点信息）
				type = Constants.ORDER_TYPE_XFHX;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				datetime = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			}
		} else if ("COFFEE:T_TASTE_ORDER".equals(tableName)) {			//咖啡
			String partner_id = columns.getString("PARTNER_ID");
			entryStation = Constants.coffeeShopMaps.get(partner_id);
			exitStation = "";
			type = Constants.ORDER_TYPE_COFFEE;
			ticketNum = 1;
			datetime = columns.getString("UPDATE_DATE");
		} else if ("SKP:SJT_QR_CODE".equals(tableName)) { 	//先付后享 非珠海（乘车码）
			entryStation = columns.getString("SJT_ENTRY_STATION_CODE");
			exitStation = columns.getString("SJT_EXIT_STATION_CODE");
			type = Constants.ORDER_TYPE_XFHX;
			ticketNum = 1;
			datetime = columns.getString("SJT_SALE_DATE");
		} else {//先享后付
			String stationCode = columns.getString("TRANS_STATION_CODE");
			type = Constants.ORDER_TYPE_XXHF;
			entryStation = stationCode;
			exitStation = stationCode;
			ticketNum = 1;
			FastDateFormat fastDateFormat2 = FastDateFormat.getInstance("yyyyMMddHHmmss");
			try {
				String dt = columns.getString("TRANS_DATE") + columns.getString("TRANS_TIME");
				datetime = fastDateFormat.format(fastDateFormat2.parse(dt));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		Station station = StationCache.getInstance().get(cityCode + "_" + entryStation);
		if(station != null){
			entryStation = station.getStationNameZh();
		} else {
			System.out.println(cityCode + "_" + entryStation);
		}
		station = StationCache.getInstance().get(cityCode + "_" + exitStation);
		if(station != null){
			exitStation = station.getStationNameZh();
		} else {
			System.out.println(cityCode + "_" + entryStation);
		}
		if(Constants.ORDER_TYPE_XXHF.equals(type)){
			if(Constants.XXHF_TYPE_ENTRY.equals(trx_type)){
				entryStation = entryStation == null ? "--" : entryStation + "(进站)";
			}
			if(Constants.XXHF_TYPE_EXIT.equals(trx_type)){
				exitStation = exitStation == null ? "--" : exitStation + "(出站)";
			}
		}
		exitStation = exitStation == null ? "---" : exitStation ;
		entryStation = entryStation == null ? "---" : entryStation;
		RedissonClient redisson =  RedisUtils.getRedissonClient();
		
		json.put("entryStation", entryStation);
		json.put("exitStation", exitStation);
		if(datetime == null){
			logger.error("获取时间为空：" + value); 
			return ;
		}
		City city = CityCache.getInstance().get(cityCode);
		json.put("cityCode", cityCode);
		json.put("cityName", city.getName());
		json.put("time", datetime);
		json.put("ticketNum", ticketNum);
		json.put("type", type);
		json.put("ticketType", Constants.orderTypeMap.get(type));
		publishLastTime(redisson, cityCode, datetime);
		redisson.getTopic(PropertyAccessor.getProperty("redis.topic.ticket")).publish(json.toJSONString());
		System.out.println(json);
	}
	
	/**
	 * 发布实时时间戳
	 * @param redisson
	 * @param cityCode
	 * @param date
	 */
	private static void publishLastTime(RedissonClient redisson, String cityCode,
			String date) {
		if(date != null){
			RBucket<String> bucket = redisson.getBucket("lastTime");
			String time = bucket.get();
			if(time != null && !"".equals(time)){
				if(date.compareTo(time) > 0){
					bucket.set(date);
					JSONObject json = new JSONObject();
					json.put("lastTime", date);
					redisson.getTopic("ticket:lastTime").publish(json.toJSONString());
				}
			} else {
				bucket.set(date);
			}
		}
	}
}
