package com.shankephone.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


/**
 * jdbc操作类
 * @author fengql
 * @version 2017年6月8日 下午3:45:41
 */
public class JdbcTemplate {
	
	private static ConnectionPool manager = ConnectionPool.getInstance();

	private JdbcTemplate(){}

	/**
	 * 执行批量操作
	 * @author fengql
	 * @date 2017年6月8日 下午3:46:27
	 * @param sql
	 * @param params
	 * @return
	 */
	public static boolean executeBatch(String sql, List<Object[]> params){
		try {
			Connection connection = manager.getConnection();
			connection.setAutoCommit(false);
			PreparedStatement pstat = connection.prepareStatement(sql);
			for(Object[] param : params){
				for(int i = 0; i < param.length; i++){
					pstat.setObject(i + 1, param[i]);
				}
				pstat.addBatch();
			}
			pstat.executeBatch();
			connection.commit();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 执行SQL语句
	 * @author fengql
	 * @date 2017年6月8日 下午3:47:19
	 * @param sql
	 * @return
	 */
	public static ResultSet executeSQL(String sql){
		Connection connection = manager.getConnection();
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = connection.prepareStatement(sql);
			return preparedStatement.executeQuery();
		} catch (Exception e){
			e.printStackTrace();
		} 
		return null;
		
	}




}
