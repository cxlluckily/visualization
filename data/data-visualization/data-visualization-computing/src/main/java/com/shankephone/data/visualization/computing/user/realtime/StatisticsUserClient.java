
package com.shankephone.data.visualization.computing.user.realtime;

import org.apache.commons.lang3.time.FastDateFormat;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.shankephone.data.visualization.computing.common.IAnalyseHandler;


public class StatisticsUserClient {
	public static void main(String[] args) throws Exception {
		FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss");
		ApplicationContext appContext =  new ClassPathXmlApplicationContext("classpath:applicationContext.xml");  
		StatisticsUserStreamingProcess userProcess = StatisticsUserStreamingProcess.getInstance();
		long timeStamp = Long.parseLong(IAnalyseHandler.getLongTimestamp(args[0]));
		userProcess.startStatistics(args.length==0?0l:timeStamp);
//		long timeStamp = Long.parseLong(IAnalyseHandler.getLongTimestamp("2017-09-12 11:00:00"));
//		userProcess.startStatistics(timeStamp);
		
	}

}
