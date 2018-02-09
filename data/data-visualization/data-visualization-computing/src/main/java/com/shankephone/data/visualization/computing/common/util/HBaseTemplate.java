package com.shankephone.data.visualization.computing.common.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class HBaseTemplate {

	private static Logger logger = LoggerFactory.getLogger(HBaseTemplate.class);
	private static Configuration conf = null;
	private static Connection conn = null;
	private static Admin admin = null;
	
	private HBaseTemplate(){}
	static {
		System.out.println("=====================static init HbaseTemplate======================");
		conf = HBaseConfiguration.create();
		// TODO 需更换为自己的zookeeper集群
		// 对于hbase客户端来说，只需要知道hbase使用的zookeeper集群地址就可以了，hbase客户端读写数据不需要知道hmaster
		conf.set(
				"hbase.zookeeper.quorum",
				"data1.test:2181,data2.test:2181,data3.test:2181,data4.test:2181,data5.test:2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		try {
			// 获取连接
			conn = ConnectionFactory.createConnection(conf);
			// 获取一个表管理器
			admin = conn.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static Configuration getHBaseConf(){
		return conf;
	}
	
	public static Connection getConnection(){
		return conn;
	}
	
	/**
	 * 在指定表中写入数据
	 * @param tableName
	 * @param columnFamily
	 * @param rowKey
	 * @param jsonList
	 */
	public static void writeTable(String tableName, String columnFamily,
			String rowKey, List<JSONObject> jsonList){
		createTableAndColumnFamily(tableName, columnFamily);
		insertColumn(tableName, columnFamily, rowKey, jsonList);
		
		System.out.println("--------------------write success!--------------------");
	}
	
	/**
	 * 表时否存在 
	 * @param tableName
	 * @return
	 */
	public static boolean existTable(String tableName){
		try {
			//通过admin获取需要的表描述器
			HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));
			if(null != htd){
				logger.info("【hbase】 hbase表["+tableName+"] 已存在!");
				return true;
			}
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println("【hbase】 hbase表["+tableName+"] 不存在!");
			logger.info("【hbase】 hbase表["+tableName+"] 不存在!");
			return false;
		}
		logger.info("【hbase】 hbase表["+tableName+"] 不存在!");
		return false;
	}
	
	/**
	 * 列族是否存在
	 * @param tableName
	 * @param columnFamily
	 * @return
	 */
	public static boolean existColumnFamily(String tableName,String columnFamily){
		try {
			//通过admin获取需要的表描述器
			HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));
			//通过表描述器获取列族描述器
			HColumnDescriptor hcd = htd.getFamily(Bytes.toBytes(columnFamily));
			if(null != hcd){
				logger.info("【hbase】 hbase表["+tableName+"] --> hbase列族["+columnFamily+"]  已存在!");
				return true;
			}
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println("【hbase】 hbase表["+tableName+"] --> hbase列族["+columnFamily+"]  不存在!");
			logger.error("【hbase】 hbase表["+tableName+"] --> hbase列族["+columnFamily+"]  不存在!");
			return false;
		}
		logger.info("【hbase】 hbase表["+tableName+"] --> hbase列族["+columnFamily+"]  不存在!");
		return false;
	}

	/**
	 * 创建表和列族
	 * @param tableName
	 * @param columnFamily
	 */
	public static void createTableAndColumnFamily(String tableName, String columnFamily) {
		if(!existTable(tableName)){
			createColumnFamilyToNewTable(tableName,columnFamily);
		}
		if(!existColumnFamily(tableName,columnFamily)){
			createColumnFamilyToExistTable(tableName,columnFamily);
		}
		//hbaseManager.createColumnFamilyToExistTable(tableName, columnFamily);
	}
	
	/**
	 * 创建列族到新表
	 * @param tableName
	 * @param columnFamily
	 * @return
	 */
	public static boolean createColumnFamilyToNewTable(String tableName,String columnFamily){
		try {
			HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
			//构建列族描述器
			HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
			hcd.setMinVersions(1);
			hcd.setMaxVersions(3);
			htd.addFamily(hcd);
			admin.createTable(htd);
			logger.info("【hbase】 创建hbase表["+tableName+"] --> hbase列族["+columnFamily+"] 成功!");
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println("【hbase】 创建hbase表["+tableName+"] --> hbase列族["+columnFamily+"] 异常!");
			logger.error("【hbase】 创建hbase表["+tableName+"] --> hbase列族["+columnFamily+"] 异常!");
			return false;
		}
		
		return true;
		
	}
	
	/**
	 * 在表中创建列族
	 * @param tableName
	 * @param columnFamily
	 * @return
	 */
	public static boolean createColumnFamilyToExistTable(String tableName,String columnFamily){
		try {
			HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));
			//构建列族描述器
			HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
			hcd.setMinVersions(1);
			hcd.setMaxVersions(3);
			htd.addFamily(hcd);
			admin.modifyTable(TableName.valueOf(tableName), htd);
			logger.info("【hbase】 创建hbase表["+tableName+"] --> hbase列族["+columnFamily+"] 成功!");
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println("【hbase】 创建hbase表["+tableName+"] --> hbase列族["+columnFamily+"] 异常!");
			logger.error("【hbase】 创建hbase表["+tableName+"] --> hbase列族["+columnFamily+"] 异常!");
			return false;
		}
		
		return true;
		
	}

	public static void insertColumn(String tableName, String columnFamily,
			String rowKey, List<JSONObject> jsonList) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			if (table == null) {
				System.out
						.println("===================table is not exists!===================");
				return;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Put> puts = new ArrayList<Put>();
		
		// key
		Put put01 = new Put(Bytes.toBytes(rowKey));
		
		jsonList.stream().forEach(r -> {
			r.keySet().stream().forEach(k -> {
				put01.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k),
						 Bytes.toBytes(r.getString(k)));
				puts.add(put01);
			});
		});
		try {
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @todo 通过过滤器获取表中数据
	 * @author sunweihong
	 * @param tableName 表名
	 * @param filter 过滤器
	 * @return
	 * 2017年4月25日 下午2:41:27
	 */
	public static JSONObject getRowsByFilters(String tableName,Filter filter){
		JSONObject resultJson = new JSONObject();
		JSONObject row = null;
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			if(filter != null){
				scan.setFilter(filter);
			}
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			while(iterator.hasNext()){
				//获取row
				Result result = iterator.next();
				//循环里面的column
				CellScanner cellScanner = result.cellScanner();
				while(cellScanner.advance()){
					Cell currentCell = cellScanner.current();
					byte[] familyArray = currentCell.getFamilyArray();
					byte[] qualifierArray = currentCell.getQualifierArray();
					byte[] valueArray = currentCell.getValueArray();
					byte[] rowArray = currentCell.getRowArray();
					String curretnRowKey = new String(rowArray, currentCell.getRowOffset(), currentCell.getRowLength());
					if(resultJson.containsKey(curretnRowKey)){
						row = resultJson.getJSONObject(curretnRowKey);
					}else{
						row = new JSONObject();
					}
					row.put(new String(familyArray,currentCell.getFamilyOffset(),currentCell.getFamilyLength()) +"@"+new String(qualifierArray,currentCell.getQualifierOffset(),currentCell.getQualifierLength()), new String(valueArray,currentCell.getValueOffset(),currentCell.getValueLength()));
					resultJson.put(curretnRowKey, row);
				}
			}
			logger.info("【hbase】 查询hbase中表为["+tableName+"]所有数据: "+resultJson.toJSONString());
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("【hbase】 查询hbase中表为["+tableName+"]所有数据，异常!");
		}
		return resultJson;
	}
	
	public static boolean close(){
		try {
			admin.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
