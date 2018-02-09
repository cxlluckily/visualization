package com.shankephone.hive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.shankephone.data.visualization.computing.common.util.HiveTemplate;

public class HiveQuery {

	/** Hive的驱动字符串 */
	
	public static void main(String[] args) throws SQLException {
		computing();
	}

	public static void computing() throws SQLException {
		Connection conn = HiveTemplate.create().getConnection();
		/*PreparedStatement pstat = conn
				.prepareStatement("select key,value['LINE_CODE'] from ext_station_code where key = '00_sttrade4100_140_4401_30_3011'");*/
		PreparedStatement pstat = conn
				.prepareStatement("select count(key) from ext_station_code");
		ResultSet rs = pstat.executeQuery();
		while (rs.next()) {
			//System.out.println(rs.getString(1) + "," + rs.getString(2));
			System.out.println(rs.getString(1));
		}
		System.out.println("成功!");
		pstat.close();
		conn.close();
	}
}
