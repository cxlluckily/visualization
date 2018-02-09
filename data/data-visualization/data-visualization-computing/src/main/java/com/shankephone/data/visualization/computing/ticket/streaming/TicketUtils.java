package com.shankephone.data.visualization.computing.ticket.streaming;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.cache.Station;
import com.shankephone.data.cache.StationCache;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.visualization.computing.common.util.Constants;

import scala.Tuple2;

/**
 * 售票量相关工具类
 * @author 森
 * @version 2017年9月15日 下午1:56:07
 */

public class TicketUtils {
	private static FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
	private final static Logger logger = LoggerFactory.getLogger(TicketUtils.class);
	
	/**
	 * 过滤有效单程票/先付后享/先享后付/咖啡/长沙圈存数据/用户
	 * 并推送票数据的站点信息
	 * @author senze
	 * @date 2018年1月25日 下午5:05:45
	 * @param value
	 * @param timestamp
	 * @return
	 */
	public static boolean filterTicketData(JSONObject value, long timestamp){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		
		if(tableName == null || columns == null) {
			return false;
		}
		
		if (timestamp != 0l) {			//离线与实时分割点
			Long exceptTimestamp = timestamp;
			Long actualTimestamp = Long.parseLong(columns.getString("T_LAST_TIMESTAMP"));
			if (actualTimestamp <= exceptTimestamp) {
				return false;
			}
		}
		
		if (StringUtils.isNotBlank(columns.getString("CITY_CODE")) 
				&& ("SKP:ORDER_INFO".equals(tableName) ||"SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName) 
									||	"COFFEE:T_TASTE_ORDER".equals(tableName) || "SKP:SJT_QR_CODE".equals(tableName) || "SKP:TR_ORDER_THRID".equals(tableName))) {
			
			
			
			String cityCode = columns.getString("CITY_CODE");
			RedissonClient redisson = RedisUtils.getRedissonClient();

			if (value.containsKey("changes") && ("SKP:ORDER_INFO".equals(tableName) || "COFFEE:T_TASTE_ORDER".equals(tableName))) {

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
								redisson.getTopic(PropertyAccessor.getProperty("redis.topic.ticket")).publish(publishData.toJSONString());				//单程票  实时交易记录
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
				
				
				if ("SKP:TR_ORDER_THRID".equals(tableName)) {     //用户数据
					String payDate = columns.getString("PAY_TIME").split(" ")[0];
					String payAccount = columns.getString("PAY_ACCOUNT");
					String state = columns.getString("STATE");
					if ("2".equals(state) && StringUtils.isNotBlank(payAccount) && StringUtils.isNotBlank(payDate)) {
						RSet<String> set = redisson.getSet("user:uniq:" + cityCode + ":" + payDate);
						return set.add(payAccount);
					}
					
				} else if ("SKP:SJT_QR_CODE".equals(tableName)) { 	//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
					String saleDate = columns.getString("SJT_SALE_DATE");
					String entryStationCode = columns.getString("SJT_ENTRY_STATION_CODE");
					String exitStationCode = columns.getString("SJT_EXIT_STATION_CODE");
					
					JSONObject changes = value.getJSONObject("changes");
					String sjt_status = columns.getString("SJT_STATUS");
					if (changes != null && sjt_status != null && changes.getString("SJT_STATUS") != null && !"03".equals(changes.getString("SJT_STATUS")) && "99".equals(sjt_status)
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
							redisson.getTopic(PropertyAccessor.getProperty("redis.topic.ticket")).publish(publishData.toJSONString());				//先享后付 实时交易记录
							return true;
						}
					} else {
						return false;
					}
					
				} else if ("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)) {		//先享后付  SKP:METRO_MEMBER_SUBSCRIPTION_TRANS （没有上下站点信息字段  record里有   表连接需求）
					FastDateFormat fastDateFormat2 = FastDateFormat.getInstance("yyyy-MM-dd");
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
							redisson.getTopic(PropertyAccessor.getProperty("redis.topic.ticket")).publish(publishData.toJSONString());				//先享后付 实时交易记录
						}
						RSet<String> set = redisson.getSet("order:uniq:" + "xxhf:" + cityCode + ":" + transDate);
						return set.add(seq+"_"+cardNum);
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * 保留统计有效信息
	 * @author senze
	 * @date 2018年1月25日 下午5:01:11
	 * @param value
	 * @return
	 */
	public static Iterator<Tuple2<Map<String,String>,Integer>> mapToUseful(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		String cityCode = columns.getString("CITY_CODE");
		String type = null;
		Integer ticketNum = 0;
		String date = null;
		
		if ("SKP:TR_ORDER_THRID".equals(tableName)) {  //用户
			type = Constants.ORDER_TYPE_USER;
			date = columns.getString("PAY_TIME").split(" ")[0];
			ticketNum = 1;
			
		} else if ("SKP:ORDER_INFO".equals(tableName) ) {
			String productCode = columns.getString("ORDER_PRODUCT_CODE");
			String productCodePre = productCode.split("_")[0];
			if ("DCP".equals(productCodePre) || "TVIP".equals(productCodePre) || "GZH".equals(productCodePre)) { 				//单程票
				type = Constants.ORDER_TYPE_DCP;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
				
			} else if ("CSDT".equals(productCode) || "NFC_SIM".equals(productCode) || "CSSH_A".equals(productCode) || "CSSH_I".equals(productCode)) {	//  长沙充值
				type = Constants.ORDER_TYPE_NFC;
				ticketNum = 1;
				date = columns.getString("TOPUP_TOPUP_DATE").split(" ")[0];
				
			} else if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//珠海有轨（无站点信息）
				type = Constants.ORDER_TYPE_XFHX;
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
			}
			
		} else if ("COFFEE:T_TASTE_ORDER".equals(tableName)) {			//咖啡
			type = Constants.ORDER_TYPE_COFFEE;
			ticketNum = 1;
			date = columns.getString("UPDATE_DATE").split(" ")[0];
			
		} else if ("SKP:SJT_QR_CODE".equals(tableName)) { 	//先付后享 非珠海（乘车码）
			type = Constants.ORDER_TYPE_XFHX;
			ticketNum = 1;
			date = columns.getString("SJT_SALE_DATE").split(" ")[0];
			
		} else {			//先享后付
			type = Constants.ORDER_TYPE_XXHF;
			ticketNum = 1;
			FastDateFormat fastDateFormat2 = FastDateFormat.getInstance("yyyy-MM-dd");
			try {
				date = fastDateFormat.format(fastDateFormat2.parse(columns.getString("TRANS_DATE")));
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}
		List<Tuple2<Map<String, String>, Integer>> result = new ArrayList<>();
		Map<String, String> map = new HashMap<>();
		Map<String, String> nationMap = new HashMap<>();
		map.put("cityCode", cityCode);
		map.put("type", type);
		map.put("date", date);
		result.add(new Tuple2<Map<String, String>, Integer>(map, ticketNum));
		nationMap.put("cityCode", "0000");
		nationMap.put("type", type);
		nationMap.put("date", date);
		result.add(new Tuple2<Map<String, String>, Integer>(nationMap, ticketNum));
		return result.iterator();
		
	}
	
	/**
	 * 统计
	 * @author senze
	 * @date 2018年1月24日 下午6:58:32
	 * @param type
	 * @param cityCode
	 * @param date
	 * @param ticketNum
	 * @param redisson
	 */
	public static void countTickets(String cityCode, String date, Map<String,Integer> detailMap, RedissonClient redisson){
		JSONObject json = new JSONObject();
		String trandeKey = "trade:vol:" + cityCode;
		RMap<String, Map<String, Integer>> tradeMap = redisson.getMap(trandeKey);
		Map<String, Integer> tradeData = tradeMap.getOrDefault(date, new HashMap<>());
		Set<String> keys=detailMap.keySet();
		for(String key : keys){
			if (!key.equals(Constants.ORDER_TYPE_USER)) {
				int lastAllNum = tradeData.get("all") == null ? 0 : tradeData.get("all");
				tradeData.put("all", lastAllNum + detailMap.get(key));
			}
			int lastOrderNum = tradeData.get(key) == null ? 0 : tradeData.get(key);
			tradeData.put(key, lastOrderNum + detailMap.get(key));
		}
		tradeMap.put(date, tradeData);
		Map<String, Integer> currentData = tradeMap.get(fastDateFormat.format(new Date()));
		if (currentData != null) {
			for(String key : currentData.keySet()){
				json.put(key, currentData.get(key));
			}
			redisson.getTopic(trandeKey).publish(json.toString());				//推送交易数据
		}
		if(!"0000".equals(cityCode)){//如果不是全国则将城市数据添加到全国
			JSONObject jsonAll = new JSONObject();
			RMap<String, Map<String, Integer>> tradeMapAll = redisson.getMap("trade:vol:0000");
			Map<String, Integer> tradeDataAll = tradeMapAll.getOrDefault(date, new HashMap<>());
			if(tradeData.containsKey("all")){
				tradeDataAll.put(cityCode, tradeData.get("all"));
			}else{
				tradeDataAll.put(cityCode, 0);
			}
			tradeMapAll.put(date, tradeDataAll);
			Map<String, Integer> dataN = tradeMapAll.get(fastDateFormat.format(new Date()));
			if (dataN != null) {
				for(String key : dataN.keySet()){
					jsonAll.put(key, dataN.get(key));
				}
				redisson.getTopic("trade:vol:0000").publish(jsonAll.toString());				//推送交易数据
			}
		}
		//处理每日售票量----开始----
		String namespace=PropertyAccessor.getProperty("redis.topic.realtime.tickets.total.namespace");
		String KEY = PropertyAccessor.getProperty("redis.topic.realtime.tickets.total.key");
		Map<String, JSONObject> map = redisson.getMap(namespace+KEY);
		JSONObject dayValue = map.get(cityCode);
		if(dayValue.containsKey(date)){//判断如果有则不操作
		}else{//如果没有，说明有新的一天的数据了，那昨天的数据肯定是完整的了，所以需要往redis推一下
			JSONObject tickets_30days=setTicketTotalByDay(redisson,dayValue);
			redisson.getTopic("tickets:30days:"+cityCode).publish(tickets_30days.toJSONString());
		}
		//处理每日售票量----结束----
	}

	//组装每日售票量数据
	public static JSONObject setTicketTotalByDay(RedissonClient redisson,JSONObject dayValue){
		JSONObject tickets_30days = new JSONObject();
		List<String> time = new ArrayList<String>();
		List<Integer> tickets = new ArrayList<Integer>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String lasttime = bucket.get();
		if(lasttime == null || "".equals(lasttime)){
			return new JSONObject();
		}
		Date today = null;
		try {
			today = sdf.parse(lasttime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		calendar.setTime(today);
		calendar.add(Calendar.DATE, -30);
		String before = sdf.format(calendar.getTime());
		int i = 0;
		while (i<30) {
			tickets.add(dayValue.getIntValue(before));
			time.add(before);
			calendar.add(Calendar.DATE, 1);
			before = sdf.format(calendar.getTime());
			i++;
		}
		tickets_30days.put("TIME", time);
		tickets_30days.put("TICKETS", tickets);
		return tickets_30days;
	}
}
