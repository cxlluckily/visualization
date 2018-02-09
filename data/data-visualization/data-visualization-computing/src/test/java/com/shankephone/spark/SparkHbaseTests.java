package com.shankephone.spark;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import com.shankephone.data.common.util.ClassUtils;

public class SparkHbaseTests {

	public static void main(String[] args) throws AnalysisException, FileNotFoundException, IOException {
		String orderInfoCatalog = IOUtils.toString(ClassUtils.getResourceAsStream("schemas/order_info.json"), "UTF-8");
		SparkSession spark = SparkSession
				.builder()
				.master("yarn")
				.appName("Java Spark Hive Example")
				.getOrCreate();
		Dataset<Row> dataset = spark.read()
								    .option(HBaseTableCatalog.tableCatalog(), orderInfoCatalog)
								    .format("org.apache.spark.sql.execution.datasources.hbase")
								    .load();
		dataset.createTempView("ORDER_INFO");
		spark.sql("select count(ROWKEY) from ORDER_INFO").show();
		spark.close();
	}
	
}
