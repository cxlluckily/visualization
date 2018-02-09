/**
 * 
 */
package com.shankephone.data.visualization.computing.ticket.streaming;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.visualization.computing.common.util.Constants;

/**      
 * @ClassName: TradeUtils  
 * @Description: 当日交易量工具类（产品）
 * @author yaoshijie  
 * @date 2018年2月1日    
 */
public class TradeUtils {
	
	private static FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
	private final static Logger logger = LoggerFactory.getLogger(TicketUtils.class);
	
	/**
	 * 
	* @Title: filterTicketData 
	* @author yaoshijie  
	* @Description: 过滤有效的交易量：闪客蜂、小程序、城市服务
	* @param @param value
	* @param @param timestamp
	* @param @return    参数  
	* @return boolean    返回类型  
	* @throws
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
		if (StringUtils.isNotBlank(columns.getString("CITY_CODE")) && columns.containsKey("PAY_PAY_TIME") && columns.containsKey("PAY_PAYMENT_TYPE")
				&& columns.getString("PAY_PAYMENT_TYPE") != null && !"".equals(columns.getString("PAY_PAYMENT_TYPE"))) {
			if (("SKP:ORDER_INFO".equals(tableName)||"SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName))) {
				return filterData(tableName,columns,value);
			}
		}
		return false;
	}
	public static boolean filterData(String tableName,JSONObject columns,JSONObject value){
		boolean statusFlag = false;
		boolean preStatusFlag = false;
		boolean ticketFlag = false;
		boolean dateFlag = false;
		String productCode = columns.getString("ORDER_PRODUCT_CODE");
		JSONObject changes=null;
		if(value.containsKey("changes")){
			changes = value.getJSONObject("changes");
		}
		if(changes!=null && "1".equals(columns.getString("ORDER_TYPE"))){//单程票
			String entryStationCode = columns.getString("TICKET_PICKUP_STATION_CODE");
			String ticketNum = columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM");
			String time = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE");
			statusFlag = "5".equals(columns.getString("TICKET_ORDER_STATUS"));
			preStatusFlag = !"5".equals(changes.getString("TICKET_ORDER_STATUS"));
			ticketFlag = StringUtils.isNotBlank(entryStationCode) && StringUtils.isNotBlank(ticketNum);
			dateFlag = StringUtils.isNotBlank(time);
		}else if(changes!=null && "2".equals(columns.getString("ORDER_TYPE"))){//长沙NFC充值
			statusFlag = "5".equals(columns.getString("TOPUP_ORDER_STATUS"));
			preStatusFlag = !"5".equals(changes.getString("TOPUP_ORDER_STATUS"));
			ticketFlag = true;
		}else if(changes!=null && "SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){//先享后付,出自 SKP:METRO_MEMBER_SUBSCRIPTION_TRANS （没有上下站点信息字段  record里有   表连接需求）
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
							&&!"05".equals(changes.getString("TICKET_STATUS")) 
							&& StringUtils.isNotBlank(entryStationCode) && StringUtils.isNotBlank(exitStationCode)) {   
					return true;
				}
			}
		}else if("4".equals(columns.getString("ORDER_TYPE"))){//先付后享
			if (changes!=null && "ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//珠海有轨（没有站点信息）
				statusFlag = "5".equals(columns.getString("TICKET_RDER_STATUS"));
				preStatusFlag = !"5".equals(changes.getString("TICKET_ORDER_STATUS"));
				ticketFlag = StringUtils.isNotBlank(columns.getString("TICKET_ACTUAL_TAKE_TICKET_NUM"));
				dateFlag = StringUtils.isNotBlank(columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE"));
			}else{//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
				String saleDate = columns.getString("XFHX_SJT_SALE_DATE");
				String entryStationCode = columns.getString("XFHX_SJT_ENTRY_STATION_CODE");
				if (!"05".equals(value.getJSONObject("changes").getString("SJT_STATUS")) && "05".equals(columns.getString("SJT_STATUS"))
						&& StringUtils.isNotBlank(columns.getString("XFHX_ORDER_NO")) && StringUtils.isNotBlank(entryStationCode)
						&& StringUtils.isNotBlank(saleDate)) {
					return true;
				} else {
					return false;
				}
			}
		}else if(changes!=null && "5".equals(columns.getString("ORDER_TYPE"))){//蜂格咖啡
			statusFlag = "4".equals(columns.getString("COFFEE_ORDER_STATE"));
			preStatusFlag = !"4".equals(changes.getString("COFFEE_ORDER_STATE"));
			ticketFlag = true;
			dateFlag = StringUtils.isNotBlank(columns.getString("COFFEE_UPDATE_DATE"));
		}
		return (statusFlag && preStatusFlag && ticketFlag && dateFlag);
	}
	
	
	/**
	 * 组装
	* @Title: mapToUseful 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param value
	* @param @return    参数  
	* @return Iterator<Map<String,Integer>>    返回类型  
	* @throws
	 */
	public static Iterator<Tuple2<Map<String,String>,Integer>> mapToUseful(JSONObject value){
		String tableName = value.getString("tableName");
		JSONObject columns = value.getJSONObject("columns");
		String cityCode = columns.getString("CITY_CODE");
		Integer num = 0;
		String type = getType(columns,tableName);
		Map<String , Object> m = getDate(tableName,columns,value);
		String date=(String) m.get("date");
		num=(Integer) m.get("ticketNum");
		
		List<Tuple2<Map<String, String>, Integer>> result = new ArrayList<>();
		Map<String, String> map = new HashMap<>();
		Map<String, String> nationMap = new HashMap<>();
		map.put("cityCode", cityCode);
		map.put("type", type);
		map.put("date", date);
		result.add(new Tuple2<Map<String, String>, Integer>(map, num));
		nationMap.put("cityCode", "0000");
		nationMap.put("type", type);
		nationMap.put("date", date);
		result.add(new Tuple2<Map<String, String>, Integer>(nationMap, num));
		return result.iterator();
	}
	public static String getType(JSONObject columns,String tableName){
		String type=null;
		String payType ="";
		if("SKP:ORDER_INFO".equals(tableName)){
			payType = columns.getString("PAY_PAYMENT_TYPE");
			if(Constants.SKF_PAY_TYPE.indexOf(payType)>=0){//闪客蜂
				type = Constants.PAY_TYPE_SKF;
			}else if(Constants.ZFB_PAY_TYPE.indexOf(payType)>=0){//支付宝城市服务
				type = Constants.PAY_TYPE_ZFB;
			}else if(Constants.YGPJ_PAY_TYPE.indexOf(payType)>=0){//云购票机
				type = Constants.PAY_TYPE_YGPJ;
			}else if(Constants.WX_PAY_TYPE.indexOf(payType)>=0){//微信小程序
				type = Constants.PAY_TYPE_WX;
			}else{//其他
				type = Constants.PAY_TYPE_QT;
			}
		}else if("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){
			payType=columns.getString("PROVIDER_ID");
			if("01".equals(payType)){//渠道编码  01：广州地铁 02：闪客蜂 11：支付宝 12：微信
				//预留类型
			}else if("02".equals(payType)){
				type = Constants.PAY_TYPE_SKF;
			}else if("11".equals(payType)){
				type = Constants.PAY_TYPE_ZFB;
			}else if("12".equals(payType)){
				type = Constants.PAY_TYPE_WX;
			}
		}
		
		return type;
	}
	public static Map<String , Object> getDate(String tableName,JSONObject columns,JSONObject value){
		Map<String , Object> map = new HashMap<String, Object>();
		String date="";
		Integer ticketNum=0;
		String productCode = columns.getString("ORDER_PRODUCT_CODE");
		if ("SKP:ORDER_INFO".equals(tableName) ) {
			if ("1".equals(columns.getString("ORDER_TYPE"))) { 				//单程票
				ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
				date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
			} else if ("2".equals(columns.getString("ORDER_TYPE"))) {	//  长沙充值
				ticketNum = 1;
				date = columns.getString("TOPUP_TOPUP_DATE").split(" ")[0];
			} else if ("4".equals(columns.getString("ORDER_TYPE"))) {		//先付后享   -- 珠海有轨（无站点信息）
				if ("ZH_RAIL_A".equals(productCode) || "ZH_RAIL_I".equals(productCode)) {		//珠海有轨（没有站点信息）
					ticketNum = columns.getInteger("TICKET_ACTUAL_TAKE_TICKET_NUM");
					date = columns.getString("TICKET_NOTI_TAKE_TICKET_RESULT_DATE").split(" ")[0];
				}else{//先付后享  非珠海（乘车码）	SKP:SJT_QR_CODE 
					ticketNum = 1;
					date = columns.getString("XFHX_SJT_SALE_DATE").split(" ")[0];
				}
			}else if("5".equals(columns.getString("ORDER_TYPE"))){
				ticketNum = 1;
				date = columns.getString("COFFEE_UPDATE_DATE").split(" ")[0];
			}
		}else if("SKP:METRO_MEMBER_SUBSCRIPTION_TRANS".equals(tableName)){//先享后付,出自 SKP:METRO_MEMBER_SUBSCRIPTION_TRANS （没有上下站点信息字段  record里有   表连接需求）
			ticketNum = 1;
			FastDateFormat fastDateFormat2 = FastDateFormat.getInstance("yyyyMMdd");
			try {
				date = fastDateFormat.format(fastDateFormat2.parse(columns.getString("TRANS_DATE")));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		map.put("date", date);
		map.put("ticketNum", ticketNum);
		return map;
	}
	
	
	/**
	 * 来源统计
	* @Title: countTickets 
	* @author yaoshijie  
	* @Description: TODO
	* @param @param type
	* @param @param cityCode
	* @param @param date
	* @param @param ticketNum
	* @param @param redisson    参数  
	* @return void    返回类型  
	* @throws
	 */
	public static void countTickets(String type, String cityCode, String date, Integer ticketNum, RedissonClient redisson){
		JSONObject json = new JSONObject();
		String trandeKey = "trade:source:vol:" + cityCode;
		RMap<String, Map<String, Integer>> tradeMap = redisson.getMap(trandeKey);
		Map<String, Integer> tradeData = tradeMap.getOrDefault(date, new HashMap<>());
		type=Constants.sourceMap_New.get(type);
		int lastOrderNum = tradeData.get(type) == null ? 0 : tradeData.get(type);
		tradeData.put(type, lastOrderNum + ticketNum);
		tradeMap.put(date, tradeData);
		
		Map<String, Integer> currentData = tradeMap.get(fastDateFormat.format(new Date()));
		if (currentData != null) {
			for(String key : currentData.keySet()){
				json.put(key, currentData.get(key));
			}
			redisson.getTopic(trandeKey).publish(json.toString());				//推送交易数据
		}
	}
	
	/*public static void main(String[] args) {
		String value="{\"columns\":{\"ORDER_REFUND_AMOUNT_DATE\":\"\",\"ORDER_ORDER_NO\":\"01201802021605037786\",\"ORDER_PAY_STATUS\":\"SUCCESS\",\"TICKET_TAKE_TICKET_SEQ_NUM\":\"\",\"ORDER_PRODUCT_CODE\":\"TVIP_NN\",\"PAY_PAYMENT_TYPE\":\"2\",\"ORDER_ORDER_SOURCE\":\"02\",\"TICKET_SINGEL_TICKET_PRICE\":\"200\",\"TICKET_NOTI_TAKE_TICKET_RESULT_DATE\":\"2018-02-02 16:05:08\",\"TICKET_REG_DATE\":\"2018-02-02 16:05:03\",\"PAY_REFUND_TIME\":\"\",\"PAY_STATE\":\"2\",\"TICKET_PICKUP_LINE_CODE\":\"01\",\"PAY_ORDER_ID\":\"A1590227061825372159\",\"TICKET_CANCLE_ORDER_DATE\":\"\",\"TICKET_EXTERNAL_ORDER_NO\":\"\",\"PAY_MODIFY_TIME\":\"2018-02-02 16:05:07\",\"PAY_SOURCE\":\"5\",\"CITY_CODE\":\"5300\",\"ORDER_TOTAL_AMOUNT\":\"200\",\"TICKET_CITY_CODE\":\"5300\",\"TICKET_ORDER_NO\":\"01201802021605037786\",\"ORDER_REFUND_AMOUNT\":\"\",\"TICKET_ACTUAL_TAKE_TICKET_NUM\":\"1\",\"PAY_CASH_AMOUNT\":\"2.0\",\"TICKET_TICKE_TYPE\":\"0\",\"PAY_COUPON_AMOUNT\":\"0.0\",\"PAY_REFUND_STATE\":\"0\",\"PAY_ENABLED\":\"1\",\"TICKET_ORDER_STATUS\":\"5\",\"ORDER_OWNER_ID\":\"010521032\",\"TICKET_PAY_CHANNEL_CODE\":\"1001\",\"PAY_TIMESTAMP\":\"1517558707000\",\"TICKET_SINGLE_TICKET_NUM\":\"1\",\"TICKET_TAKE_TICKET_TOKEN\":\"TE43F8C82B4BB4EC3952856A24750D8F3\",\"ORDER_REG_DATE\":\"2018-02-02 16:05:03\",\"ORDER_ORDER_DATE\":\"2018-02-02 16:05:03\",\"PAY_TOTAL_AMOUNT\":\"2.0\",\"TICKET_GETOFF_LINE_CODE\":\"\",\"ORDER_SURCHARGE_AMOUNT\":\"\",\"PAY_REFUND_AMOUNT\":\"0.0\",\"TICKET_TIMESTAMP\":\"1517558713000\",\"ORDER_TYPE\":\"1\",\"PAY_PAY_TIME\":\"2018-02-02 16:05:08\",\"ORDER_PAYMENT_DATE\":\"2018-02-02 16:05:07\",\"PAY_PAY_ACCOUNT\":\"2088802918277770\",\"ORDER_DELETE_YN\":\"N\",\"TICKET_GETOFF_STATION_CODE\":\"0000\",\"PAY_ORDER_NO\":\"01201802021605037786\",\"T_LAST_TIMESTAMP\":\"1517558714013\",\"ORDER_ID\":\"ACHD10FCE05247D4BE4A4440694100CFDE3\",\"TICKET_ID\":\"STE8D46DCE217A4FDB82C350FCA5FDD89E\",\"ORDER_TIMESTAMP\":\"1517558707000\",\"ORDER_REFUND_MANAGER_ID\":\"\",\"TICKET_PICKUP_STATION_CODE\":\"0105\",\"PAY_CREATE_TIME\":\"2018-02-02 16:05:06\"},\"changes\":{\"TICKET_ORDER_STATUS\":\"4\",\"T_LAST_TIMESTAMP\":\"1517558711009\",\"TICKET_NOTI_TAKE_TICKET_RESULT_DATE\":\"\",\"TICKET_ACTUAL_TAKE_TICKET_NUM\":\"0\"},\"rowkey\":\"6032_5300_01201802021605037786\",\"schemaName\":\"STTRADE5300\",\"tableName\":\"SKP:ORDER_INFO\"}";
		JSONObject jso=JSON.parseObject(value);//json字符串转换成jsonobject对象
//		System.out.println(filterTicketData(jso, 0l));
		System.out.println(mapToUseful(jso));
	}*/
}
