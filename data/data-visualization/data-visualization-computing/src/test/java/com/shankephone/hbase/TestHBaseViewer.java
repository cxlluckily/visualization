package com.shankephone.hbase;

import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;

public class TestHBaseViewer {
	
	private Configuration conf = null;
	private Connection conn = null;
	private Admin admin = null;
	
	private String tableName = "computing_table_7100_2";
	
	/**
	 * @todo 初始化连接
	 * @author sunweihong
	 * @throws Exception
	 * 2017年4月13日 下午5:41:27
	 */
	@Before
	public void init() throws Exception{
		conf = HBaseConfiguration.create();
		//TODO 需更换为自己的zookeeper集群
		//对于hbase客户端来说，只需要知道hbase使用的zookeeper集群地址就可以了，hbase客户端读写数据不需要知道hmaster
//		conf.set("hbase.zookeeper.quorum", "hadoop05:2181,hadoop06:2181,hadoop07:2181");
		conf.set("hbase.zookeeper.quorum", "data1.shankephone.com:2181,data2.shankephone.com:2181,data6.shankephone.com:2181,data7.shankephone.com:2181,data8.shankephone.com:2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");
		//获取连接
		conn = ConnectionFactory.createConnection(conf);
		//获取一个表管理器
		admin = conn.getAdmin();
	}
	
	@Test
	public void testScan() throws Exception{
		Date start = new Date();
		
		Table t_user_info = conn.getTable(TableName.valueOf(tableName));

		Scan scan = new Scan();
		ResultScanner scanner = t_user_info.getScanner(scan);

		Iterator<Result> iter = scanner.iterator();
		int i = 0;
		while (iter.hasNext()) {
			Result result = iter.next();
			CellScanner cellScanner = result.cellScanner();
			while (cellScanner.advance()) {
				Cell current = cellScanner.current();
				byte[] familyArray = current.getFamilyArray();
				byte[] valueArray = current.getValueArray();
				byte[] qualifierArray = current.getQualifierArray();
				byte[] rowArray = current.getRowArray();
				
				i++;

				System.out.print(new String(rowArray, current.getRowOffset(), current.getRowLength())+"   ");
				System.out.print(new String(familyArray, current.getFamilyOffset(), current.getFamilyLength()));
				System.out.print(":" + new String(qualifierArray, current.getQualifierOffset(), current.getQualifierLength()));
				System.out.println(" " + new String(valueArray, current.getValueOffset(), current.getValueLength()));
			}
			System.out.println("--------------------------------------------------------------------");
		}
		Date end = new Date();
		long len = end.getTime() - start.getTime();
		System.out.println("-------------" + i + "-------------" + len);
		
	}
	
	@Test
	public void testDrop() throws Exception {
		String table = "order_test";
		if(admin.isTableEnabled(TableName.valueOf(table))){
			admin.disableTable(TableName.valueOf(table));
		}
		admin.deleteTable(TableName.valueOf(table));
		
		/*admin.disableTable(TableName.valueOf("computing_table_22"));
		admin.deleteTable(TableName.valueOf("computing_table_22"));
		admin.disableTable(TableName.valueOf("computing_test_1"));
		admin.deleteTable(TableName.valueOf("computing_test_1"));*/
	}
	
	@Test
	public void testgetRowsByFilters(){
		FilterList filterList2 = new FilterList(Operator.MUST_PASS_ALL);
		SingleColumnValueFilter scvf3 = new SingleColumnValueFilter(Bytes.toBytes("owner_order"), Bytes.toBytes("ORDER_SOURCE"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("00"))); 
		scvf3.setFilterIfMissing(true);
		scvf3.setLatestVersionOnly(true);
		SingleColumnValueFilter scvf4 = new SingleColumnValueFilter(Bytes.toBytes("owner_order"), Bytes.toBytes("PAY_STATUS"), CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("SUCCESS"))); 
		scvf4.setFilterIfMissing(true);
		scvf4.setLatestVersionOnly(true);
		filterList2.addFilter(scvf3);
		filterList2.addFilter(scvf4);
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		filterList.addFilter(filterList2);
		
		JSONObject resultJson = new JSONObject();
		JSONObject row = null;
		try {
			Table table = conn.getTable(TableName.valueOf("owner_order"));
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			int i = 0; 
			while(iterator.hasNext() && i++ < 10){
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
					System.out.println(row.toJSONString());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
