package com.shankephone.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import com.shankephone.data.common.util.PropertyAccessor;

/**
 * 连接池类
 * @author fengql
 * @version 2017年6月8日 下午3:51:59
 */
public class ConnectionPool {

	private static ConnectionPool pool;
	private static Properties props;

	//连接池默认初始化5个连接
	private static int initSize = 5;
	//获取连接为空时，默认重试3次
	private static int retryTimes = 3;
	//最大连接数,未使用
	//private static int maxSize = 20;
	
	private static Queue<Connection> queue = new ArrayBlockingQueue<Connection>(
			initSize);
	private String url;
	private String username;
	private String password;
	private String driver;
	private ConnectionPool() {
		props = PropertyAccessor.getProperties("dataSource");
		driver = props.getProperty("driverClassName");
		url = props.getProperty("url");
		username = props.getProperty("username");
		password = props.getProperty("password");
		try {
			Class.forName(driver);
			for (int i = 0; i < initSize; i++) {
				Connection connection = DriverManager.getConnection(url,username,password);
				queue.offer(connection);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取数据库连接
	 * @author fengql
	 * @date 2017年6月8日 下午3:32:41
	 * @return
	 */
	public Connection getConnection() {
		//从队列中取出连接
		Connection conn = queue.poll();
		//如果未取到等待2秒
		int retry = retryTimes;
		while (conn == null) {
			//如果超过重试次数，返回null
			if(retry == 0){
				break;
			}
			try {
				Thread.currentThread();
				//等待2秒
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
			conn = queue.poll();
			retry--;
		}
		return conn;
	}

	/**
	 * 关闭连接，放回连接池
	 * @author fengql
	 * @date 2017年6月8日 下午3:38:38
	 * @param conn
	 */
	public void close(Connection conn) {
		if (conn != null) {
			//连接放回连接池
			boolean result = queue.offer(conn);
			//如果连接池已满，则关闭连接
			if (!result) {
				try {
					conn.close();
				} catch (SQLException e) {
					conn = null;
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 清空连接池
	 * @author fengql
	 * @date 2017年6月8日 下午3:40:24
	 */
	public void clear() {
		for (int i = 0; i < initSize; i++) {
			Connection connection = queue.poll();
			try {
				connection.close();
			} catch (SQLException e) {
				connection = null;
				e.printStackTrace();
			}
		}
	}

	/**
	 * 获取连接池
	 * @author fengql
	 * @date 2017年6月8日 下午3:40:45
	 * @return
	 */
	public static synchronized ConnectionPool getInstance() {
		if (pool == null) {
			pool = new ConnectionPool();
		}
		return pool;
	}

	

}
