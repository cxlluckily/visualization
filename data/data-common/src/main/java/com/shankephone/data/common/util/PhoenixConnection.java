package com.shankephone.data.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixConnection {
	/**
	 * @todo phoenix连接对象
	 */
	private Connection conn = null;
	
	/**
	 * @todo 机制对象
	 */
	private  Logger logger = LoggerFactory.getLogger(PhoenixConnection.class);
	

	/**
	 * 私有构造方法，只能在本类使用
	 */
	private PhoenixConnection() {
		phoenixConnection();
	}
	
	
	/** 
	 * @todo 提供一个静态的方法来获取hbase的连接对象
	 * @author sunweihong
	 * @date 2017年6月8日 上午9:48:17
	 * @return
	 */
	public static PhoenixConnection getInstance(){
		return new PhoenixConnection();
	}


	/**
	 * @todo 连接hbase服务器
	 * @author sunweihong
	 * 2017年4月18日 上午10:56:13
	 */
	private void phoenixConnection(){
		try {
			Class.forName(PropertyAccessor.getProperty("phoenix.driver"));
			final ExecutorService exec = Executors.newFixedThreadPool(1);
			Callable<Connection> call = new Callable<Connection>() {
				public Connection call() throws Exception {
					Properties pro = PropertyAccessor.getProperties("phoenixconf");
					return DriverManager.getConnection(PropertyAccessor.getProperty("phoenix.url"),pro);
				}
			};
			Future<Connection> future = exec.submit(call);
			// 如果在15s钟之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞，防止服务夯死
			conn = future.get(1000 * 15, TimeUnit.MILLISECONDS);
			exec.shutdownNow();
			logger.info("【hbase】 连接phoenix，成功！");
		} catch (Exception e) {
			logger.info("【hbase】 连接phoenix，异常！");
		}
	}
	
	public boolean close(){
		try {
			if(conn!=null){
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}


	public Connection getConn() {
		return conn;
	}
}
