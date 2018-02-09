package com.shankephone.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.visualization.computing.common.util.HBaseTemplate;
import com.shankephone.jdbc.JdbcTemplate;

public class ComputingOffline implements Serializable{
	private static final long serialVersionUID = 1L;
	private String appName = "computing-amount";
	private String master = "local[4]";
	private String tableName = "owner_order";
	private SparkConf sparkConf;
	private JavaSparkContext context;
	private Configuration hbaseConf;

	public ComputingOffline(){
		hbaseConf = HBaseConfiguration.create();
		hbaseConf
				.set("hbase.zookeeper.quorum",
						"data0.test:2181,data1.test:2181,data2.test:2181,data3.test:2181,data4.test:2181,data5.test:2181");
		hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");
		sparkConf = new SparkConf().setAppName(appName).setMaster(master);
		context = new JavaSparkContext(sparkConf);
	}

	public static void main(String[] args) {
		ComputingOffline offline = new ComputingOffline();
		offline.process();
	}

	public FilterList generateFilter() {
		FilterList filterList2 = new FilterList(Operator.MUST_PASS_ONE);
		SingleColumnValueFilter scvf3 = new SingleColumnValueFilter(
				Bytes.toBytes("basic"), Bytes.toBytes("ORDER_SOURCE"),
				CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("00")));
		scvf3.setFilterIfMissing(true);
		scvf3.setLatestVersionOnly(true);
		SingleColumnValueFilter scvf4 = new SingleColumnValueFilter(
				Bytes.toBytes("basic"), Bytes.toBytes("ORDER_SOURCE"),
				CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("01")));
		scvf4.setFilterIfMissing(true);
		scvf4.setLatestVersionOnly(true);
		filterList2.addFilter(scvf3);
		filterList2.addFilter(scvf4);
		/*
		 * FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		 * filterList.addFilter(filterList2);
		 */
		return filterList2;
	}

	public void getRowsByFilters() {
		FilterList filterList = generateFilter();
		JSONObject resultJson = new JSONObject();
		try {
			Table table = HBaseTemplate.getConnection().getTable(
					TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				JSONObject row = null;
				// 获取row
				Result result = iterator.next();
				if (result == null) {
					continue;
				}
				// 循环里面的column
				CellScanner cellScanner = result.cellScanner();
				while (cellScanner.advance()) {
					Cell currentCell = cellScanner.current();
					byte[] familyArray = currentCell.getFamilyArray();
					byte[] qualifierArray = currentCell.getQualifierArray();
					byte[] valueArray = currentCell.getValueArray();
					byte[] rowArray = currentCell.getRowArray();
					String curretnRowKey = new String(rowArray,
							currentCell.getRowOffset(),
							currentCell.getRowLength());
					if (resultJson.containsKey(curretnRowKey)) {
						row = resultJson.getJSONObject(curretnRowKey);
					} else {
						row = new JSONObject();
					}
					row.put(new String(familyArray, currentCell
							.getFamilyOffset(), currentCell.getFamilyLength())
							+ "@"
							+ new String(qualifierArray, currentCell
									.getQualifierOffset(), currentCell
									.getQualifierLength()), new String(
							valueArray, currentCell.getValueOffset(),
							currentCell.getValueLength()));
					resultJson.put(curretnRowKey, row);
					JSONObject json = new JSONObject();
					json.put(
							new String(qualifierArray, currentCell
									.getQualifierOffset(), currentCell
									.getQualifierLength()), new String(
									valueArray, currentCell.getValueOffset(),
									currentCell.getValueLength()));
				}
				System.out.println(row.toJSONString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println(resultJson.toJSONString());
	}

	public String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}

	public void process() {
		try {
			Scan scan = new Scan();
			scan.setFilter(generateFilter());
			hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
			hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan));
			JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = context
					.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
							ImmutableBytesWritable.class, Result.class);
			long total = hBaseRDD.map(r -> {
				Result result = r._2();
				byte[] amountByte = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("TOTAL_AMOUNT"));
				String amount = Bytes.toString(amountByte);
				//byte[] owner = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("OWNER_ID"));
				//String ownerId = Bytes.toString(owner);
				//System.out.println(ownerId + "--" + amount);
				long count = Long.parseLong(amount);
				return count;
			}).reduce((a,b) -> {
				//System.err.println(a + " + " + b + " = " + (a + b));
				return a + b;
			}).longValue();
			writeResult(total);
			// 任务结束
			context.stop();
			System.out.println("==========   END.....  =========");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void writeResult(long total) {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String day = sdf.format(date);
		String sql = "insert into computing_amount(compute_time, total_amount) values(?, ?)";
		List<Object[]> params = new ArrayList<Object[]>();
		params.add(new Object[]{day, total});
		JdbcTemplate.executeBatch(sql, params);
	}

}
