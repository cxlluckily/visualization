package com.shankephone.data.visualization.computing.common.util;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import com.shankephone.data.common.util.ClassUtils;


public class SparkSQLTemplate {
	
	private static final String SCHEMA_PATH = "schemas/";
	private SparkSession session;
	private String catalog;
	
	public SparkSQLTemplate(String appName, String master, String catalogName){
		try {
			this.catalog = IOUtils.toString(ClassUtils.getResourceAsStream(SCHEMA_PATH + catalogName), "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.session = SparkSession
				.builder()
				//master缺省值是yarn
				.master((master == null || "".equals(master)) ? "yarn" : master)
				.appName(appName)
				.getOrCreate();
	}
	
	public SparkSession getSession(){
		return session;
	}
	
	public SparkSession createSession(String appName, String master){
		session = SparkSession
				.builder()
				.master((master == null || "".equals(master)) ? "yarn" : master)
				.appName(appName)
				.getOrCreate();
		return session;
	}
	
	public Dataset<Row> load(String tableName){
		Dataset<Row> dataset = session.read()
			    .option(HBaseTableCatalog.tableCatalog(), catalog)
			    .format("org.apache.spark.sql.execution.datasources.hbase")
			    .load();
		try {
			dataset.createTempView(tableName);
		} catch (AnalysisException e) {
			e.printStackTrace();
		}
		return dataset;
	}
	
	public Dataset<Row> query(String tableName, String sql) {
		Dataset<Row> dataset = session.read()
			    .option(HBaseTableCatalog.tableCatalog(), catalog)
			    .format("org.apache.spark.sql.execution.datasources.hbase")
			    .load();
		try {
			dataset.createTempView(tableName);
		} catch (AnalysisException e) {
			e.printStackTrace();
		}
		return session.sql(sql);
	}
	
	public Dataset<Row> query(String sql) {
		String tableName = getTableName(sql);
		Dataset<Row> dataset = session.read()
			    .option(HBaseTableCatalog.tableCatalog(), catalog)
			    .format("org.apache.spark.sql.execution.datasources.hbase")
			    .load();
		try {
			dataset.createTempView(tableName);
		} catch (AnalysisException e) {
			e.printStackTrace();
		}
		return session.sql(sql);
	}
	
	public static String getTableName(String sql){
		sql = sql.toUpperCase();
		int idx1 = sql.lastIndexOf("FROM");
		sql = sql.substring(idx1 + 4).trim();
		int idx2 = sql.indexOf(" ");
		String tableName = sql.substring(0, idx2).trim();
		return tableName;
	}
	
	public static void main(String[] args) {
		String sql = " select CITY_CODE,count(CITY_CODE) from ORDER_INFO group by CITY_CODE ";
		System.out.println(SparkSQLTemplate.getTableName(sql));
	}
	

}
