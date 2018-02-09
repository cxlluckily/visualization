package com.shankephone.data.visualization.computing.ticket.offline;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;
import com.shankephone.data.visualization.computing.common.util.Constants;


/**
 * 支付渠道分析
 * @author fengql
 * @version 2017年8月22日 下午5:06:09
 */
public class TicketPaymentAnalyse implements Executable{
		
	private final static Logger logger = LoggerFactory.getLogger(TicketPaymentAnalyse.class);
	
	@Override
	public void execute(Map<String, String> argsMap) {
		logger.info("======================analyse start===========================");
		String timestamp = argsMap == null ? null : argsMap.get("endTime");
		analysePayment("ticket_payment", timestamp);
		logger.info("======================analyse end===========================");
	}
	
	public void analysePayment(String sqlFile, String timestamp){
		logger.info("=========================start==========================");
		try {
			//返回long型字符串
			String time = IAnalyseHandler.getLongTimestamp(timestamp);
			if(time == null || "".equals(time)){
				throw new RuntimeException("离线任务-支付渠道，没有截止时间戳！");
			}
			Long end = Long.parseLong(time);
			Date today = new Date();
			
			//pay_pay_time数据时间限制
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("lastTime", end + "");
			params.put("day", "'" + sdf.format(today) + "'");
			SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
			Dataset<Row> results = builder.executeSQL(sqlFile, params);
			processCity(results);
			builder.getSession().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("=========================end==========================");
	}
	
	private void processCity(Dataset<Row> dataset) {
		List<Row> rows = dataset.collectAsList();
		//结果集按城市计算
		RedissonClient redisson = RedisUtils.getRedissonClient();
		Map<String,Map<String,Map<String,Integer>>> dataMap = new HashMap<String,Map<String,Map<String,Integer>>>();
		for(Row r : rows){
			String city_code = r.getString(0);
			String day = r.getString(1);
			if(city_code == null || "".equals(city_code)){
				/*logger.warn("城市代码为空！错误数据：[city_code:" + city_code + 
						", day:" + day + ", payment_code:" + payment_code = ", ticket_num:" + num
						"]");*/
				logger.warn("城市代码为空：" + r.toString()); 
				continue;
			}
			if(day == null || "".equals(day)){
				logger.warn("日期为空:" + r.toString()); 
				continue;
			
			}
			String payment_code = r.getString(2);
			Integer num = (Double)r.getDouble(3) == null ? 0 : ((Double)r.getDouble(3)).intValue();
			
			//获取城市的所有天的支付数据
			Map<String,Map<String,Integer>> cityMap = dataMap.get(city_code);
			if(cityMap == null){
				cityMap = new HashMap<String,Map<String,Integer>>();
			}
			Map<String,Integer> paymentMap = cityMap.get(day);
			if(paymentMap == null){
				paymentMap = new HashMap<String,Integer>();
			}
			//放入支付数据
			paymentMap.put(Constants.paymentMaps.get(payment_code),num);
			cityMap.put(day, paymentMap);
			dataMap.put(city_code, cityMap);
		}
		Set<String> keySet = dataMap.keySet();
		for(String cityCode : keySet){
			Map<String,Map<String,Integer>> cityMap = dataMap.get(cityCode);
			Set<String> daySet = cityMap.keySet();
			//写入缓存
			String cacheName = Constants.REDIS_NAMESPACE_TICKET_PAYMENT 
					+ cityCode;
			for(String day : daySet){
				Map<String,Integer> dayMap = cityMap.get(day);
				redisson.getMap(cacheName).put(day, dayMap);
				logger.warn("put redis: " + day + "-" + dayMap);
				JSONObject json = JSONObject.parseObject(JSON.toJSONString(dayMap));
				redisson.getTopic(cacheName).publish(json);
				logger.warn("publish redis " + cacheName + ": " + json.toJSONString());
			}
		}
	}
	
	public static void main(String[] args) {
		TicketPaymentAnalyse analyse = new TicketPaymentAnalyse();
		Map<String,String> argsMap = new HashMap<String,String>();
		argsMap.put("endTime", "2018-02-08 13:40:00");
		analyse.execute(argsMap);
	}
}
