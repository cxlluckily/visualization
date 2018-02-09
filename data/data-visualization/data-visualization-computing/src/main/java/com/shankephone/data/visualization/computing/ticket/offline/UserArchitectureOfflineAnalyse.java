package com.shankephone.data.visualization.computing.ticket.offline;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.visualization.computing.common.IAnalyseHandler;
import com.shankephone.data.visualization.computing.common.util.Constants;



/**
 * 订单来源分析
 * @author fengql
 * @version 2017年8月22日 下午5:06:09
 */
public class UserArchitectureOfflineAnalyse implements Executable{
		
	private final static Logger logger = LoggerFactory.getLogger(UserArchitectureOfflineAnalyse.class);
	
	@Override
	public void execute(Map<String, String> argsMap) {
		logger.info("======================order-source-offline start===========================");
		String timestamp = argsMap == null ? null : argsMap.get("endTime");
		analyseOthers("order_source",timestamp);
		logger.info("======================order-source-offline end===========================");
		
	}
	
	public void analyseOthers(String sqlFile, String timestamp){
		logger.info("=========================analyseOthers start==========================");
		try {
			timestamp = IAnalyseHandler.getLongTimestamp(timestamp);
			if(timestamp == null || "".equals(timestamp)){
				throw new RuntimeException("离线任务-订单来源，没有截止时间戳！");
			}
			//结束时间
			Long end = Long.parseLong(timestamp);
			//开始时间
			Date startDate = new Date(end);
			
			//pay_pay_time数据时间限制
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("lastTime", end + "");
			params.put("day", "'" + sdf.format(startDate) + "'");
			
			SparkSQLBuilder builder = new SparkSQLBuilder(this.getClass());
			Dataset<Row> rows = builder.executeSQL(sqlFile, params);
			
			processDataset(rows,builder.getSession());
			//IAnalyseHandler.publishData(timestamp, Constants.REDIS_TOPIC_USER_ARCHITECTURE);
			builder.getSession().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("=========================analyseOthers end==========================");
	}
	
	public void processDataset(Dataset<Row> rows, SparkSession session){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		List<Row> list = rows.collectAsList();
		for(Row r : list){
			String days = r.getString(0);
			Object skp = r.get(1);
			Object skpwx = r.get(2);
			Object track = r.get(3);
			Object ali = r.get(4);
			Object hb = r.get(5);
			Object cld = r.get(6);
			Object esurfing = r.get(7);
			
			Long skpc = skp == null ? 0l : (Long)skp;
			Long skpwxc = skpwx == null ? 0l : (Long)skpwx;
			Long trackc = track == null ? 0l : (Long)track;
			Long alic = ali == null ? 0l : (Long)ali;
			Long hbc = hb == null ? 0l : (Long)hb;
			Long cldc = cld == null ? 0l : (Long)cld;
			Long esurfingc = esurfing == null ? 0l : (Long)esurfing;
			
			JSONObject rowJSON = new JSONObject();
			if(skpc.intValue() != 0){
				JSONObject arc = new JSONObject();
				arc.put("name", Constants.architectureMap.get(Constants.ANALYSE_REGISTER_SHANKEPHONE));
				arc.put("value", skpc.intValue());
				rowJSON.put(Constants.ANALYSE_REGISTER_SHANKEPHONE, arc);
			}
			if(skpwxc.intValue() != 0){
				JSONObject arc = new JSONObject();
				arc.put("name", Constants.architectureMap.get(Constants.ANALYSE_REGISTER_SHANKEPHONE_WECHAT));
				arc.put("value", skpwxc.intValue());
				rowJSON.put(Constants.ANALYSE_REGISTER_SHANKEPHONE_WECHAT, arc);
			}
			if(trackc.intValue() != 0){
				JSONObject arc = new JSONObject();
				arc.put("name", Constants.architectureMap.get(Constants.ANALYSE_REGISTER_TRACK));
				arc.put("value", trackc.intValue());
				rowJSON.put(Constants.ANALYSE_REGISTER_TRACK, arc);
			}
			if(alic.intValue() != 0){
				JSONObject arc = new JSONObject();
				arc.put("name", Constants.architectureMap.get(Constants.ANALYSE_REGISTER_ALI));
				arc.put("value", alic.intValue());
				rowJSON.put(Constants.ANALYSE_REGISTER_ALI, arc);
			}
			if(hbc.intValue() != 0){
				JSONObject arc = new JSONObject();
				arc.put("name", Constants.architectureMap.get(Constants.ANALYSE_REGISTER_HEB));
				arc.put("value", hbc.intValue());
				rowJSON.put(Constants.ANALYSE_REGISTER_HEB, arc);
			}
			if(cldc.intValue() != 0){
				JSONObject arc = new JSONObject();
				arc.put("name", Constants.architectureMap.get(Constants.ANALYSE_REGISTER_CLOUD));
				arc.put("value", cldc.intValue());
				rowJSON.put(Constants.ANALYSE_REGISTER_CLOUD, arc);
			}
			if(esurfingc.intValue() != 0){
				JSONObject arc = new JSONObject();
				arc.put("name", Constants.architectureMap.get(Constants.ANALYSE_REGISTER_TY));
				arc.put("value", esurfingc.intValue());
				rowJSON.put(Constants.ANALYSE_REGISTER_TY, arc);
			}
			
			//按天写入缓存
			RMap<String,JSONObject> rm = 
					redisson.getMap(
							Constants.REDIS_NAMESPACE_USER_ARCHIVITECTURE + 
							Constants.REDIS_TOPIC_USER_ARCHITECTURE + 
							Constants.DIMENSION_SEPERATOR + days);
			rm.put("all", rowJSON);
			String topic = Constants.REDIS_TOPIC_USER_ARCHITECTURE;
			redisson.getTopic(topic).publish(rowJSON);
			logger.info("--------发布成功！--------");
			logger.info(rowJSON.toJSONString());
		}
		
		
	}
}
