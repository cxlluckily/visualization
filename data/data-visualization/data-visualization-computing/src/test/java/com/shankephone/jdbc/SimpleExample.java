
package com.shankephone.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.test.SpringTestCase;
import com.shankephone.data.common.util.PropertyAccessor;

public class SimpleExample extends SpringTestCase{
	@Resource
	private RedissonClient redisson;
	
	@Test
	public void loadCityLineStationRelations(){
		boolean flag = true;
		
		try {
			JSONObject result = new JSONObject();
			List<String> schemaList = new ArrayList<String>();
			schemaList.add("sttrade");
			schemaList.add("sttrade1200");
			schemaList.add("sttrade2660");
			schemaList.add("sttrade4100");
			schemaList.add("sttrade4500");
			schemaList.add("sttrade5300");
			schemaList.add("sttrade7100");
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			Class.forName("com.mysql.jdbc.Driver") ;
			Connection conn = DriverManager.getConnection(
							"jdbc:mysql://172.10.2.93:3306/sttrade?useUnicode=true&amp;characterEncoding=utf-8",
							"report", "!QAZxcvfr432");
//			Connection conn = DriverManager.getConnection(PropertyAccessor.getProperty("dataSource.url") , PropertyAccessor.getProperty("dataSource.username") , PropertyAccessor.getProperty("dataSource.password") ) ; 
			if ("true".equals(PropertyAccessor.getProperty("redis.load.citylinestationrelations"))) {
				for (String schema : schemaList) {
					String sql = "select "
							+ "l.CITY_CODE,l.LINE_CODE,l.LINE_NAME_EN,l.LINE_NAME_ZH,s.STATION_CODE,s.STATION_NAME_EN,s.STATION_NAME_ZH "
							+ "from " + schema + ".station_code s LEFT JOIN "+ schema + ".line_code l on s.LINE_CODE=l.LINE_CODE";

					pstmt = conn.prepareStatement(sql);
					rs = pstmt.executeQuery(sql);
					while (rs.next()) {
						String CITY_CODE = rs.getString(1);
						String LINE_CODE = rs.getString(2);
						String LINE_NAME_EN = rs.getString(3);
						String LINE_NAME_ZH = rs.getString(4);
						String STATION_CODE = rs.getString(5);
						String STATION_NAME_EN = rs.getString(6);
						String STATION_NAME_ZH = rs.getString(7);
						JSONObject city = null;
						JSONObject station = null;

						if (result.containsKey(CITY_CODE)) {
							city = result.getJSONObject(CITY_CODE);
							if (!city.containsKey(STATION_CODE)) {
								station = new JSONObject();
								station.put("CITY_CODE", CITY_CODE);
								station.put("LINE_CODE", LINE_CODE);
								station.put("LINE_NAME_EN", LINE_NAME_EN);
								station.put("LINE_NAME_ZH", LINE_NAME_ZH);
								station.put("STATION_CODE", STATION_CODE);
								station.put("STATION_NAME_EN", STATION_NAME_EN);
								station.put("STATION_NAME_ZH", STATION_NAME_ZH);
								city.put(STATION_CODE, station);
							}
						} else {
							city = new JSONObject();
							station = new JSONObject();
							station.put("CITY_CODE", CITY_CODE);
							station.put("LINE_CODE", LINE_CODE);
							station.put("LINE_NAME_EN", LINE_NAME_EN);
							station.put("LINE_NAME_ZH", LINE_NAME_ZH);
							station.put("STATION_CODE", STATION_CODE);
							station.put("STATION_NAME_EN", STATION_NAME_EN);
							station.put("STATION_NAME_ZH", STATION_NAME_ZH);
							city.put(STATION_CODE, station);
						}

						result.put(CITY_CODE, city);
					}
				}
				System.out.println(result.toJSONString());
				RBucket<String> relations = redisson.getBucket(PropertyAccessor.getProperty("redis.citylinestationrelations.key"));
				relations.set(result.toJSONString());
			} else {
				if (redisson.getBucket(PropertyAccessor.getProperty("redis.citylinestationrelations.key")).isExists()) {
					for (String schema : schemaList) {
						String sql = "select "
								+ "l.CITY_CODE,l.LINE_CODE,l.LINE_NAME_EN,l.LINE_NAME_ZH,s.STATION_CODE,s.STATION_NAME_EN,s.STATION_NAME_ZH "
								+ "from " + schema
								+ ".station_code s LEFT JOIN " + schema
								+ ".line_code l on s.LINE_CODE=l.LINE_CODE";
						pstmt = conn.prepareStatement(sql);
						rs = pstmt.executeQuery(sql);
						while (rs.next()) {
							String CITY_CODE = rs.getString(1);
							String LINE_CODE = rs.getString(2);
							String LINE_NAME_EN = rs.getString(3);
							String LINE_NAME_ZH = rs.getString(4);
							String STATION_CODE = rs.getString(5);
							String STATION_NAME_EN = rs.getString(6);
							String STATION_NAME_ZH = rs.getString(7);
							JSONObject city = null;
							JSONObject station = null;

							if (result.containsKey(CITY_CODE)) {
								city = result.getJSONObject(CITY_CODE);
								if (!city.containsKey(STATION_CODE)) {
									station = new JSONObject();
									station.put("CITY_CODE", CITY_CODE);
									station.put("LINE_CODE", LINE_CODE);
									station.put("LINE_NAME_EN", LINE_NAME_EN);
									station.put("LINE_NAME_ZH", LINE_NAME_ZH);
									station.put("STATION_CODE", STATION_CODE);
									station.put("STATION_NAME_EN",
											STATION_NAME_EN);
									station.put("STATION_NAME_ZH",
											STATION_NAME_ZH);
									city.put(STATION_CODE, station);
								}
							} else {
								city = new JSONObject();
								station = new JSONObject();
								station.put("CITY_CODE", CITY_CODE);
								station.put("LINE_CODE", LINE_CODE);
								station.put("LINE_NAME_EN", LINE_NAME_EN);
								station.put("LINE_NAME_ZH", LINE_NAME_ZH);
								station.put("STATION_CODE", STATION_CODE);
								station.put("STATION_NAME_EN", STATION_NAME_EN);
								station.put("STATION_NAME_ZH", STATION_NAME_ZH);
								city.put(STATION_CODE, station);
							}
							result.put(CITY_CODE, city);
						}
					}
					System.out.println(result.toJSONString());
					RBucket<String> relations = redisson.getBucket(PropertyAccessor.getProperty("redis.citylinestationrelations.key"));
					relations.set(result.toJSONString());
					
				}
			}
			
			
			
			if(rs != null)
		     {   // 关闭记录集   
		        try{   
		            rs.close() ;   
		        }catch(SQLException e){   
		            e.printStackTrace() ;   
		        }   
		     }   
		     if(pstmt != null)
		     {   // 关闭声明   
		        try{   
		        	pstmt.close() ;   
		        }catch(SQLException e){   
		            e.printStackTrace() ;   
		        }   
		     }   
		     if(conn != null)
		     {  // 关闭连接对象   
		         try{   
		            conn.close() ;   
		         }catch(SQLException e){   
		            e.printStackTrace() ;   
		         }   
		     } 
			
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
			
		}   
			
		
//		return flag;
		
	}
	
}

