package com.shankephone.data.storage.spark;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.computing.Executable;
import com.shankephone.data.common.hbase.HBaseClient;
import com.shankephone.data.common.kafka.KafkaHelper;
import com.shankephone.data.common.spark.SparkStreamingBuilder;
import com.shankephone.data.common.spark.SparkStreamingBuilder.KafkaOffset;
import com.shankephone.data.storage.common.ConfigureUtil;
import com.shankephone.data.storage.config.StorageConfig;
import com.shankephone.data.storage.exception.SyncException;
import com.shankephone.data.storage.pojo.ConfigTable;

/**
 * 数据同步类，接收Kafka中的流数据，同步到Kafka和HBase中
 * @author fengql
 * @version 2018年1月22日 上午10:42:47
 */
public class SyncTaskExecutor implements Executable {
	
	private static Logger logger = LoggerFactory.getLogger(SyncTaskExecutor.class);
	
	private static boolean debug = false;
	
	//配置文件名
	private static final String fileName = "storageConfig.xml";
	//配置对象
	private static StorageConfig storageConfig = null;
	//应用标识，也是groupId
	private static String appName = "dataStorage";
	
	//默认构造，生产使用
	public SyncTaskExecutor() {}
	
	//测试构造
	public SyncTaskExecutor(String fileName) {
		debug = true;
		storageConfig = ConfigureUtil.loadConfigure(fileName);
	}
	
	static {
		debug = false;
		storageConfig = ConfigureUtil.loadConfigure(fileName);
	}
	
	@Override
	public void execute(Map<String, String> args) {
		SyncTaskExecutor task = new SyncTaskExecutor();
		task.processStreaming();
	}
	
	public static HBaseClient getHBaseClient(){
		return HBaseClient.getInstance();
	}
	
	public static Connection getConnection(){
		return HBaseClient.getInstance().getConnection();
	}
	
	public void  processStreaming(String... timestamps) {
		String timestamp = null;
		if(timestamps != null && timestamps.length > 0){
			timestamp = timestamps[0];
		}
		try {
			SparkStreamingBuilder builder = new SparkStreamingBuilder(appName);
			JavaStreamingContext streamingContext = builder.getStreamingContext();
			JavaInputDStream<ConsumerRecord<String, String>> stream;
			if(timestamp == null || "".equals(timestamp)){
				if(debug){
					stream = builder.createKafkaStream(KafkaOffset.END);
				} else {
					stream = builder.createKafkaStream(KafkaOffset.END);
				}
			} else {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date date;
				try {
					date = sdf.parse(timestamp);
				} catch (ParseException e) {
					e.printStackTrace();
					throw new SyncException(e);
				}
				stream = builder.createKafkaStream(date.getTime());
			}
			
			stream.foreachRDD(r -> {
				OffsetRange[] offsetRanges = ((HasOffsetRanges) r.rdd()).offsetRanges();
				try {
					/*
					 * TODO 异常捕获-待处理
					 * 在offsetRanges范围获取不到数据（数据可能已经消费）时，要保证程序正常运行。
					 */
					r.foreachPartition(f -> {
						/*OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
						System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());*/
						while(f.hasNext()){
							ConsumerRecord<String,String> next = f.next();
							String key = next.key();
							JSONObject value = JSONObject.parseObject(next.value());
							try {
								executeSync(key,value);
							} catch (Exception e) {
								e.printStackTrace();
								throw new SyncException(e, value.toJSONString());
							}
						}
					});	
					((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
				} catch (Exception e) {
					e.printStackTrace();
					//TODO 待插入日志中
					throw new RuntimeException(e);
				}
					
			});
			streamingContext.start();
			System.out.println("---------------------start---------------------");
			streamingContext.awaitTermination();
			System.out.println("---------------------end---------------------");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void executeSync(String key,JSONObject value){
		try {
			String event = value.getString("eventTypeName");
			switch(event){
				case "insert":
					insertOrUpdate(key, value, event);
					break;
				case "update":
					insertOrUpdate(key, value, event);
					break;
				case "delete":
					delete(value);
					break;
				//TODO truncate事件，暂不处理
				default:
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new SyncException(e, "数据同步操作异常！", value);
		}
		
	}
	

	/**
	 * 数据插入与更新，数据写入kafka和hbase
	 * @param key
	 * @param value
	 * @param event
	 */
	public static void insertOrUpdate(String key,JSONObject value, String event){
		try {
			String tableName = value.getString("tableName");
			//配置中的所有表映射
			Map<String, List<ConfigTable>> tableMap = storageConfig.getTableMap();
			/*//配置中的所有HBase聚合表名称 
			List<String> aggregations = storageConfig.getAggregations();*/
			//如果配置中有该表
			if(tableMap.containsKey(tableName)){
				List<ConfigTable> tableNodes = tableMap.get(tableName);
				for(ConfigTable tableNode : tableNodes){
					String hbaseTableName = tableNode.getHbase();
					String columnFamily = tableNode.getColumnFamily();
					String topic = tableNode.getTopic();
					if(!ConfigureUtil.checkCriteria(tableNode, value)){
						continue;
					}
					Tuple2<Map<String,String>,Map<String,String>> allColsMappings = ConfigureUtil.getColumnMapped(tableNode, value);
					//获取当前表的所有字段与值的映射，字段前缀根据配置进行添加
					Map<String, String> columnsMapping = allColsMappings._2;
					//获取rowkey
					String rowkey = ConfigureUtil.getRowkey(key, tableNode, allColsMappings);
					//获取改变的列
					JSONObject updateJSON = getUpdateJSON(tableNode, rowkey, columnsMapping);
					//如果是聚合表
					//if(aggregations.contains(hbaseTableName)){
						//判断是否进行更新操作
						if(updateJSON != null && !updateJSON.isEmpty() && updateJSON.size() > 0){
							//获取hbase的table对象
							Table table = getConnection().getTable(TableName.valueOf(hbaseTableName));
							Put put = new Put(Bytes.toBytes(rowkey));
							Set<String> columnsSet = columnsMapping.keySet();
							for(String k : columnsSet){
								String columnValue = columnsMapping.get(k) == null ? "" : String.valueOf(columnsMapping.get(k));
								put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(columnValue));
							}
							table.put(put);
							JSONObject json = new JSONObject();
			        	    json.putAll(columnsMapping);
			        	    if(debug){
			        	    	System.out.println("写入Hbase: " + tableNode.getHbase() + " ---" + rowkey + "---" + json.toJSONString());
			        	    }
							//写回topic, key使用rowkey
							//updateJSON.put(Constants.RECIEVE_JSON_KEY_NAME, value.get(Constants.RECIEVE_JSON_KEY_NAME));
							updateJSON.put("tableName", tableNode.getHbase());
							updateJSON.put("schemaName", value.getString("schemaName"));
							updateJSON.put("rowkey", rowkey);
							if(topic != null && !"".equals(topic)){
								if(debug){
									System.out.println("send--" + rowkey + "---" + tableNode.getHbase() + "--" + updateJSON.toJSONString());
								}
								KafkaHelper.buildProducer(appName).send(new ProducerRecord<String, String>(topic, key, updateJSON.toJSONString()),
							       		 (metadata, e) -> {
							       			 if (e != null) {
							       				 throw new SyncException(e, "发送数据异常", updateJSON);
							       			 } else {
							       				//logger.info("【hbase】 数据插入kafka的["+topic+"]成功! "+orderInfo.toJSONString());
							       			 }
							       		 }
							    );
							}
						}
					/*}else{
						//如果是单表复制
						if(updateJSON != null && !updateJSON.isEmpty()){
							//获取hbase的table对象
							Table table = getConnection().getTable(TableName.valueOf(hbaseTableName));
							Put put = new Put(Bytes.toBytes(rowkey));
							Set<String> columnsSet = columnsMapping.keySet();
							for(String k : columnsSet){
								String columnValue = columnsMapping.get(k)==null ? "" : String.valueOf(columnsMapping.get(k));
								put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(columnValue));
							}
							table.put(put);
							
							if(topic != null && !"".equals(topic)){
								updateJSON.put(Constants.RECIEVE_JSON_KEY_NAME, value.get(Constants.RECIEVE_JSON_KEY_NAME));
								updateJSON.put("tableName", tableNode.getHbase());
								//重新定义变量，供以下回调使用
								KafkaHelper.createProducer().send(new ProducerRecord<String, String>(topic, rowkey, updateJSON.toJSONString()),
						       		 (metadata, e) -> {
						       			 if (e != null) {
						       				e.printStackTrace();
						       				throw new SyncException(e, "发送数据异常！", updateJSON);
						       			 } else {
						       				//logger.info("【hbase】 数据重新插入kafka的["+topic+"]成功! "+value.toJSONString());
						       			 }
						       		 });
							}
						}
					}*/
				}
			}
		} catch (Exception e) {
			throw new SyncException(e, value);
		}
	}
	
	/**
	 * 获取已改变值的所有列，如果没有改变返回null,如果有改变，返回存储所有改变列和值的JSONObject
	 * @param tableNode
	 * @param rowKey
	 * @param columnsMapping
	 * @return
	 */
	private static JSONObject getUpdateJSON(ConfigTable tableNode, String rowKey,
			Map<String, String> columnsMapping) {
		JSONObject json = new JSONObject();
		JSONObject columns = new JSONObject();
		JSONObject changeColumns = new JSONObject();
		String tableName = tableNode.getHbase();
		String columnPrefix = tableNode.getColumnPrefix();
		//根据rowkey获取数据
		JSONObject singleRowByKeyValue = getHBaseClient().getSingleRowByKeyValue(tableName, rowKey);
		if(singleRowByKeyValue.isEmpty() && singleRowByKeyValue.size() == 0){
			//如果没有数据，需要进行插入
			json.put("columns", columnsMapping);
		}else{
			//查找表中的执行时间列，如果有前缀，则对应前缀列，无前缀则对应T_TIMESTAMP。
			String timeStampKey = (columnPrefix == null || "".endsWith(columnPrefix) ? "T_" : columnPrefix)+"TIMESTAMP";
			String timestampValue = singleRowByKeyValue.getString(timeStampKey);
			if(timestampValue == null || "".equals(timestampValue)){
				//如果返回的数据中timestamp为空，则插入
				json.put("columns", columnsMapping);
				columns.putAll(singleRowByKeyValue);
				columns.putAll(columnsMapping);
			}else{
				//如果timestamp列有值，需要和接收的数据进行比对
				Long remoteTimeStamp = Long.parseLong(timestampValue);
				Long localTimeStamp = Long.parseLong(String.valueOf(columnsMapping.get(timeStampKey)));
				//如果接收数据中的时间戳大于等于hbase数据的时间戳：["+(localTimeStamp >= remoteTimeStamp)+"],remoteTimeStamp="+remoteTimeStamp+",localTimeStamp="+localTimeStamp);
				if(localTimeStamp >= remoteTimeStamp){
					Set<String> keySet = columnsMapping.keySet();
					for(String key : keySet){
						String localValue = (String)columnsMapping.get(key);
						String remoteValue = singleRowByKeyValue.getString(key) == null ? "" : singleRowByKeyValue.getString(key);
						//判断所有列的值是否有改变,即:不是该表的timestamp列（执行时间列），且其它列的数据不相等将认为数据有变化
						if(!key.equals(timeStampKey) && !remoteValue.equals(localValue)){
							changeColumns.put(key, remoteValue);
						}
					}
					if(!changeColumns.isEmpty() && changeColumns.size() > 0){
						json.put("changes", changeColumns);
						columns.putAll(singleRowByKeyValue);
						columns.putAll(columnsMapping);
					}
				}
			}
			json.put("columns", columns);
		}
		return json;
	}

	/**
	 * 同步删除数据
	 * @param value 接收的数据
	 */
	public static void delete(JSONObject value){
		try {
			String tableName = value.getString("tableName");
			//配置中的所有表映射
			Map<String, List<ConfigTable>> tableMap = storageConfig.getTableMap();
			//原表与历史表映射
			Map<String, String> historyMap = storageConfig.getHistoryMap();
			//如果配置文件中
			if(tableMap.containsKey(tableName)){
				//获取对应的映射规则table对象
				List<ConfigTable> tableNodes =tableMap.get(tableName);
				String rowkey = "";
				int count = 0;
				for(int i = 0; i < tableNodes.size(); i++){
					ConfigTable tableNode = tableNodes.get(i);
					//获取当前表的所有字段与值的映射，字段根据配置已加前缀
					Tuple2<Map<String,String>,Map<String,String>> allColsMappings = ConfigureUtil.getColumnMapped(tableNode, value);
					//Map<String, String> originalColsMapping = allColsMappings._1;
					Map<String, String> columnsMapping = allColsMappings._2;
					rowkey = ConfigureUtil.getRowkey(null, tableNode, allColsMappings);
					boolean flag = canDelete(tableName, historyMap, rowkey);
		            if(flag){
		            	/*List<String> aggregations = storageConfig.getAggregations();
		            	if(!aggregations.contains(tableName)){
		            		deleteByRowKey(tableNode, rowkey);
		            	} else {*/
		            		delete(tableNode, columnsMapping, rowkey);
		            	//}
		        	    JSONObject json = new JSONObject();
		        	    json.putAll(columnsMapping);
		        	    if(debug){
		        	    	System.out.println("已删除--" + tableNode.getHbase() + "---" + rowkey);
		        	    }
		        	    count++;
		            } 
				}
				//如果没有任务表删除，则删除历史表
				if(count == 0){
					//如果不删除任何一个表，说明是切分数据，则删除历史表中数据
	            	String historyName = historyMap.get(tableName);
	            	//获取历史表
	            	ConfigTable table = storageConfig.getSingleTableMap().get(historyName);
	            	deleteByRowKey(table, rowkey);
	            	if(debug){
	            		System.out.println("已删除历史表--" + table.getHbase() + "--" + rowkey);
	            	}
				}
			}
		} catch (Exception e) {
			throw new SyncException(e, "hbase同步delete失败！", value);
		}
	}

	/**
	 * 根据rowkey删除表数据
	 * @param tableNode
	 * @param columnsMapping
	 * @param rowkey
	 * @throws IOException
	 */
	private static void deleteByRowKey(ConfigTable tableNode,
			String rowkey)
			throws IOException {
		String tableName = tableNode.getMysql();
		String columnFamily = tableNode.getColumnFamily();
		Table table = getConnection().getTable(TableName.valueOf(tableName));
		try {
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			delete.addFamily(Bytes.toBytes(columnFamily));
			table.delete(delete);
			table.close();
		} catch (Exception e) {
			throw new SyncException(e, "删除失败！");
		} finally {
			table.close();
		}
	}
	
	/**
	 * 删除列族中的字段
	 * @param tableNode
	 * @param columnsMapping
	 * @param rowkey
	 * @throws IOException
	 */
	private static void delete(ConfigTable tableNode,
			Map<String, String> columnsMapping, String rowkey)
			throws IOException {
		String tableName = tableNode.getHbase();
		String columnFamily = tableNode.getColumnFamily();
		Table table = getConnection().getTable(TableName.valueOf(tableName));
		try {
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			Set<String> columnsSet = columnsMapping.keySet();
			for(String k : columnsSet){
				delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k));
			}
			table.delete(delete);
		} catch (Exception e) {
			throw new SyncException("删除失败！");
		} finally {
			table.close();
		}
	}

	/**
	 * 是否删除
	 * @param tableName
	 * @param historyMap
	 * @param rowkey
	 * @return
	 * @throws IOException
	 */
	private static boolean canDelete(String tableName,
			Map<String, String> historyMap, String rowkey) throws IOException {
		//默认不删除
		boolean flag = false;
		String historyName = historyMap.get(tableName);	
		if(historyName == null){
			return true;
		}
		Table historyTable = getConnection().getTable(TableName.valueOf(historyName));
		if(historyTable == null){
			logger.warn("表【" + historyTable + "】没有历史表！");
			return true;
		}
		Get scan = new Get(rowkey.getBytes());// 根据rowkey查询  
		Result rs = historyTable.get(scan);  
		if(!rs.isEmpty()){
			if(rs.rawCells().length>0){
				//如果根据rowkey在历史表中找到数据，则不删除
		    	flag = false;
		    } 
		} else {
			//如果在历史表中未找到，则删除
			flag = true;
		}
		return flag;
	}
	
	
	public static void main(String[] args) {
		SyncTaskExecutor task = new SyncTaskExecutor("storageConfig-test.xml");
		task.processStreaming();
	}

}
