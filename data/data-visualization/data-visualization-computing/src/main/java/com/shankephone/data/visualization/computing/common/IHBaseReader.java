package com.shankephone.data.visualization.computing.common;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shankephone.data.common.util.ClassUtils;
import com.shankephone.data.visualization.computing.common.util.Constants;
import com.shankephone.data.visualization.computing.common.util.StringUtil;

/**
 * spark读取HBase接口
 * @author fengql
 * @version 2017年8月24日 下午5:52:11
 */
public interface IHBaseReader {
	
	public final static Logger logger = LoggerFactory.getLogger(IHBaseReader.class);
	
	public static String getCatalog(String schemaFile){
		try {
			String catalog = IOUtils.toString(ClassUtils.getResourceAsStream("schemas/" + schemaFile), "UTF-8");
			return catalog;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static String getSQL(String sqlFile, String timestamp){
		try {
			String sql = IOUtils.toString(ClassUtils.getResourceAsStream("sqls/" + sqlFile), "UTF-8");
			sql = IHBaseReader.resolve(sql, timestamp);
			return sql;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static String getSQL(String sqlFile, Long start, Long end){
		try {
			String sql = IOUtils.toString(ClassUtils.getResourceAsStream("sqls/" + sqlFile), "UTF-8");
			sql = IHBaseReader.resolve(sql, start, end);
			return sql;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Dataset<Row> getDatasetBySchema(SparkSession session, String schemaFile) {
		String catalog = IHBaseReader.getCatalog(schemaFile);
		Dataset<Row> dataset = session.read()
			    .option(HBaseTableCatalog.tableCatalog(), catalog)
			    .format("org.apache.spark.sql.execution.datasources.hbase")
			    .load();
		return dataset;
	}
	
	public static Dataset<Row> getDatasetByCatalog(SparkSession session, String catalog) {
		Dataset<Row> dataset = session.read()
			    .option(HBaseTableCatalog.tableCatalog(), catalog)
			    .format("org.apache.spark.sql.execution.datasources.hbase")
			    .load();
		return dataset;
	}

	public static  SparkSession createSparkSession(String appName){
		return SparkSession
		.builder()
		.master("yarn")
		.appName(appName)
		.getOrCreate();
	}
	
	public static String resolve(String sql, Long start, Long end){
		sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_PREFIX, "");
		sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_SUFFIX, "");
		if(start != null && sql.contains("$[1]")){
			sql = sql.replace("$[1]", "'" + start.toString() + "'");
		}
		if(end != null && sql.contains("$[2]")){
			sql = sql.replace("$[2]", "'" + end.toString() + "'");
		}
		logger.info("---------resolve sql success:---------" + sql);
		return sql;
	}
	
	public static String resolve(String sql, String start, String end){
		sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_PREFIX, "");
		sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_SUFFIX, "");
		sql = sql.replace("$[1]", "'" + start.toString() + "'");
		sql = sql.replace("$[2]", "'" + end.toString() + "'");
		logger.info("---------resolve sql success:---------" + sql);
		return sql;
	}

	public static String resolve(String sql, String timestamp){
		logger.info("---------------------------" + timestamp + "--------------------------");
		if(null == timestamp || "".equals(timestamp)){
			sql = sql.replaceAll(Constants.SQL_PLACEHOLDER_REGULAR, "");
			logger.info(sql);
			logger.info("-----------------------------------------------------");
			return sql;
		}
		boolean digital = StringUtil.onlyDigital(timestamp);
		Long time = 0l;
		if(digital){
			//timestamp为long型
			time = Long.parseLong(timestamp);
			if(time == null || 0l == time.longValue()){
				sql = sql.replaceAll(Constants.SQL_PLACEHOLDER_REGULAR, "");
			} else {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date date = new Date();
				date.setTime(time);
				String fmt = sdf.format(date);
				sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_PREFIX, "");
				sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_SUFFIX, "");
				sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_PARAM, "'" + fmt + "'");
			}
		} else {
			//timestamp为时间格式
			sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_PREFIX, "");
			sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_SUFFIX, "");
			sql = sql.replace(Constants.SQL_PLACEHOLDER_REGULAR_PARAM, "'" + timestamp + "'");
		}
		logger.info(sql);		
		logger.info("-----------------------------------------------------");
		return sql;
	}
}
