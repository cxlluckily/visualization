package com.shankephone.data.visualization.computing.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Hive查询模板
 * 
 * @author fengql
 * @version 2017年6月6日 下午3:04:49
 */
public class HiveTemplate {

	private static HiveTemplate template;

	private static String driver;
	private static String url;
	private static String username;
	private static String password;

	private static Properties properties = new Properties();

	static {
		InputStream in = HiveTemplate.class.getClassLoader()
				.getResourceAsStream("application.properties");
		try {
			properties.load(in);
			driver = properties.getProperty("hive.driver");
			url = properties.getProperty("hive.url");
			username = properties.getProperty("hive.username");
			password = properties.getProperty("hive.password");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private HiveTemplate() {
	}

	public static synchronized HiveTemplate create() {
		if (template == null) {
			template = new HiveTemplate();
		}
		return template;
	}

	public Connection getConnection() {
		try {
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, username,
					password);
			return conn;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<Map<String, Object>> query(String sql) throws SQLException {
		Connection conn = HiveTemplate.create().getConnection();
		PreparedStatement pstat = conn.prepareStatement(sql);
		try {
			ResultSet rs = pstat.executeQuery();
			ResultSetMetaData metadata = rs.getMetaData();
			int count = metadata.getColumnCount();
			List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> recordMap = new HashMap<String, Object>();
				for (int i = 1; i <= count; i++) {
					String name = metadata.getColumnName(i);
					int type = metadata.getColumnType(i);
					Object o = getColumnValueType(rs, type, i);
					//System.out.println(name + ":" + o) ;
					recordMap.put(name, o);
				}
				result.add(recordMap);
			}
			pstat.close();
			conn.close();
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(pstat != null){
				pstat.close();
			}
			if(conn != null){
				conn.close();
			}
		}
		return null;
	}

	private Object getColumnValueType(ResultSet rs, int type, int idx)
			throws SQLException {
		Object o = null;
		switch (type) {
		case Types.CHAR:
			o = rs.getString(idx);
			break;
		case Types.VARCHAR:
			o = rs.getString(idx);
			break;
		case Types.BIGINT:
			o = rs.getLong(idx);
			break;
		case Types.INTEGER:
			o = rs.getInt(idx);
			break;
		case Types.DOUBLE:
			o = rs.getDouble(idx);
			break;
		case Types.FLOAT:
			o = rs.getFloat(idx);
			break;
		case Types.DECIMAL:
			o = rs.getBigDecimal(idx);
			break;
		case Types.DATE:
			o = rs.getDate(idx);
			break;
		case Types.TIMESTAMP:
			o = rs.getTimestamp(idx);
			break;
		default:
			o = null;
			break;
		}
		return o;
	}

}
