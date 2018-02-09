package com.shankephone.data.visualization.computing.common.util;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 定义ApplicationContext类
 * 用于获取spring管理的Bean
 * @author fengql
 * @version 2017年6月9日 上午9:06:31
 */
public class ComputingApplicationContext {
	
	//配置文件名
	private static String configLocations = null;
	//加载配置文件
	private static ClassPathXmlApplicationContext appContext = null;
	
	private ComputingApplicationContext(){
		configLocations = "applicationContext.xml";
		appContext = new ClassPathXmlApplicationContext(configLocations);
	}
	
	private static ComputingApplicationContext context = null;
	
	public static synchronized ComputingApplicationContext getInstance(){
		if(context == null){
			context = new ComputingApplicationContext();
		}
		return context;
	}
	
	/**
	 * 获取Bean
	 * @author fengql
	 * @date 2017年6月9日 上午9:07:58
	 * @param beanName
	 * @return
	 */
	public Object getBean(String beanName){
		return appContext.getBean(beanName);
	}
	

}
