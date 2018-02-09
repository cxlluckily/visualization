package com.shankephone.data.visualization.computing.common.util;

import java.util.HashMap;
import java.util.Map;

public class Constants {
	
	/**
	 * 先享后付交易类型：53.进站，54.出站
	 */
	public static final String XXHF_TYPE_ENTRY = "53";
	public static final String XXHF_TYPE_EXIT = "54";
	
	/*订单类型*/
	public static Map<String,String> orderTypeMap = new HashMap<String,String>();
	public static final String ORDER_TYPE_DCP = "dcp";  		//单程票
	public static final String ORDER_TYPE_XXHF = "xxhf";		//先享后付
	public static final String ORDER_TYPE_XFHX = "xfhx";		//先付后享
	public static final String ORDER_TYPE_NFC = "nfc";		//长沙NFC
	public static final String ORDER_TYPE_COFFEE = "coffee";	//蜂格咖啡
	public static final String ORDER_TYPE_USER = "user";	//用户信息
	static {
		orderTypeMap.put("dcp","单程票");
		orderTypeMap.put("xxhf","先享后付");
		orderTypeMap.put("xfhx","先付后享");
		orderTypeMap.put("nfc","长沙NFC");
		orderTypeMap.put("coffee","蜂格咖啡");
	}
	
	public static Map<String,String> coffeeShopMaps = new HashMap<String,String>();
	static {
		coffeeShopMaps.put("510000002_2009","南洲店");
		coffeeShopMaps.put("510000002_2005","赤岗店");
		coffeeShopMaps.put("510000002_2003","三元里店");
		coffeeShopMaps.put("510000002_2008","公园前店");
		coffeeShopMaps.put("510000002_2006","晓港店");
		coffeeShopMaps.put("510000002_2007","芳村店");
		coffeeShopMaps.put("510000002_2002","体育西路店");
		coffeeShopMaps.put("510000002_2004","琶洲店");
	}
	
	
	public static final String PAYMENT_TYPE_ALI = "0";
	public static final String PAYMENT_TYPE_WX = "1";
	public static final String PAYMENT_TYPE_ZYD = "2";
	public static final String PAYMENT_TYPE_YZF = "3";
	public static final String PAYMENT_TYPE_SX = "4";
	public static final String PAYMENT_TYPE_YL = "5";
	public static final String PAYMENT_TYPE_PC = "9";
	public static final String PAYMENT_TYPE_QT = "99";
	
	public static final String PAY_TYPE_WX = "wx";
	public static final String PAY_TYPE_SKF = "skf";
	public static final String PAY_TYPE_ZFB = "zfb";
	public static final String PAY_TYPE_YGPJ = "ygpj";
	public static final String PAY_TYPE_QT = "qt";
	
	/*来源区分*/
	public static final String SKF_PAY_TYPE="0,1,3,5,8,9,11";
	public static final String WX_PAY_TYPE="12,13,14,7";
	public static final String ZFB_PAY_TYPE="6";
	public static final String YGPJ_PAY_TYPE="2,4";
	
	public static Map<String,String> sourceMap_New = new HashMap<String,String>();
	static {
		sourceMap_New.put("skf","闪客蜂APP");
		sourceMap_New.put("wx","微信小程序");
		sourceMap_New.put("zfb","支付宝城市服务");
		sourceMap_New.put("ygpj","云购票机");
		sourceMap_New.put("qt","其它");
	}
	
	public static Map<String,String> sourceMap = new HashMap<String,String>();
	static {
		sourceMap.put("1","购票机（TVIP设备）");
		sourceMap.put("2","闪客蜂APP");
		sourceMap.put("3","闪客蜂公众号");
		sourceMap.put("4","地铁公众号");
		sourceMap.put("5","支付宝城市服务或支付宝服务窗");
		sourceMap.put("99","其它");
	}
	
	public static Map<String,String> marchantMap = new HashMap<String,String>();
	static {
		sourceMap.put("510000001","4401");
		sourceMap.put("450000001","4500");
		sourceMap.put("530000001","5300");
		sourceMap.put("300000001","1200");
		sourceMap.put("410000001","4100");
	}
	
	public static Map<String,String> cityMap = new HashMap<String,String>();
	static {
		cityMap.put("1200", "天津");
		cityMap.put("2660", "青岛");
		cityMap.put("4100", "长沙");
		cityMap.put("4401", "广州");
		cityMap.put("4500", "郑州");
		cityMap.put("5300", "南宁");
		cityMap.put("7100", "西安");
		cityMap.put("5190", "珠海");
		cityMap.put("0000", "全国");
	}
	
	/**
	 * 统计时段类型：0-小时，1-天，2-周，3-月，4-年
	 */
	public final static Integer PERIOD_TYPE_HOUR = 0;
	public final static Integer PERIOD_TYPE_DATE = 1;
	public final static Integer PERIOD_TYPE_WEEK = 2;
	public final static Integer PERIOD_TYPE_MONTH = 3;
	public final static Integer PERIOD_TYPE_YEAR = 4;
	
	
	
	/**
	 * 根维度代码
	 */
	public final static String DIMENSION_CODE_ROOT = "0000";
	
	/**
	 * 维度分隔符
	 */
	public final static String DIMENSION_SEPERATOR = "_";
	
	/**
	 * 数据分析支持的最小年份
	 */
	public final static Integer MIN_YEAR = 2012;
	
	/**
	 * 数据库视图
	 */
	//全部、来源、支付视图
	public final static String DATABASE_VIEW_TICKET_PAYMENT = "v_ticket_payment";
	//单程票视图
	public final static String DATABASE_VIEW_TICKET_SINGLE = "v_ticket_single";
	//圈存视图
	public final static String DATABASE_VIEW_TOPUP_CARD = "v_ticket_topup_card";
	
	public final static Map<String,String> architectureMap = new HashMap<String,String>();
	static {
		architectureMap.put("1100", "闪客蜂APP");
		architectureMap.put("1200", "闪客蜂公众号");
		architectureMap.put("1300", "地铁公众号");
		architectureMap.put("1400", "支付宝");
		architectureMap.put("1500", "和包");
		architectureMap.put("1600", "云购票机");
		architectureMap.put("1700", "天翼支付");
		architectureMap.put("1999", "其它");
	}
	
	/**
	 * 用户体系表统计二级分类: 闪客蜂、闪客蜂微信公众号、轨道微信公众号、支付宝城市服务、和包城市服务、云闸机支付
	 */
	public final static String ANALYSE_REGISTER_SHANKEPHONE = "1100";
	public final static String ANALYSE_REGISTER_SHANKEPHONE_WECHAT = "1200";
	public final static String ANALYSE_REGISTER_TRACK = "1300";
	public final static String ANALYSE_REGISTER_ALI = "1400";
	public final static String ANALYSE_REGISTER_HEB = "1500";
	public final static String ANALYSE_REGISTER_CLOUD = "1600";
	public final static String ANALYSE_REGISTER_TY = "1700";
	public final static String ANALYSE_REGISTER_CHANGSHA_APP = "1800";
	public final static String ANALYSE_REGISTER_QT = "1999";
	
	/**
	 * 用户数量统计：一级分类：2000，二级分类：新用户-2100，老用户-2200
	 */
	public final static String ANALYSE_USER_NUM = "2000";
	public final static String ANALYSE_USER_NUM_NEW = "2100";
	public final static String ANALYSE_USER_NUM_OLD = "2200";
	
	/**
	 * 用户体系表统计视图: 第三方支付，云闸机支付，长沙地铁APP-单程票闪客蜂长沙，闪客蜂非长沙，天翼支付非长沙
	 */

	public final static String HIVE_VIEW_REGISTER_PAYMENT = "v_register_payment";
	public final static String HIVE_VIEW_REGISTER_CLOUDY = "v_register_cloudy";
	public final static String HIVE_VIEW_REGISTER_CHANGSHA_APP = "pr_customer";
	public final static String HIVE_VIEW_REGISTER_CHANGSHA_TY = "v_register_changsha_ty";
	public final static String HIVE_VIEW_REGISTER_SKP_CHANGSHA = "v_register_skp_changsha";
	public final static String HIVE_VIEW_REGISTER_SKP_OTHER = "v_register_skp_other";
	public final static String HIVE_VIEW_REGISTER_BESTPAY_OTHER = "v_register_bestpay_other";
	public final static String HIVE_VIEW_REGISTER_USERS = "v_register_users";
	public final static String HIVE_VIEW_REGISTER_CHANGSHA = "v_register_changsha";
	
	/**
	 * 首周最少天数
	 */
	public final static Integer FIRST_WEEK_MIN_DAYS = 4;
	/**
	 * 是否补充缺失时段的数据，默认不补充
	 */
	public final static boolean FILL_ABSENT_PERIOD_DATA = false;
	
	/**
	 * 固定省份代码-长沙
	 */
	public final static String CITY_CODE_CHANGSHA = "4100";
	
	/**
	 * 运营商
	 */
	public final static String MNO_CM = "10";         	//移动
	public final static String MNO_CT = "20";				//电信
	public final static String MNO_CU = "30";				//联通
	public final static String MNO_OTHER = "40";
	
	/**
	 * 用户体系表分析列
	 */
	public final static String REGISTER_COLUMN_SKP_CM = "skp_cm";
	public final static String REGISTER_COLUMN_SKP_CT = "skp_ct";
	public final static String REGISTER_COLUMN_SKP_CU = "skp_cu";
	public final static String REGISTER_COLUMN_SKP_OTHER = "skp_other";
	
	public final static String REGISTER_COLUMN_SKP_WECHAT = "skp_wechat";
	public final static String REGISTER_COLUMN_TRACK_WECHAT = "track_wechat";
	public final static String REGISTER_COLUMN_ALI_CS = "ali_cs";
	public final static String REGISTER_COLUMN_HB_CS = "hb_cs";	
	public final static String REGISTER_COLUMN_CLOUD_MACHINE = "cloud_mc";
	
	public final static String REGISTER_COLUMN_ESURFING_CM = "esurfing_cm";
	public final static String REGISTER_COLUMN_ESURFING_CT = "esurfing_ct";
	public final static String REGISTER_COLUMN_ESURFING_CU = "esurfing_cu";
	
	
	
	/**
	 * 根据最小粒度数据进行合并统计
	 */
	public final static boolean ANALYSE_COMBINE_OPEN = true;
	
	public final static boolean ANALYSE_DATA_TEST_SCOPE = true;
	
	public final static String SPLIT_SYMBOL = "#";
	
	/**
	 * sql语句占位符
	 */
	public final static String SQL_PLACEHOLDER_REGULAR = "#\\{.+\\}";
	public final static String SQL_PLACEHOLDER_REGULAR_PREFIX = "#{";
	public final static String SQL_PLACEHOLDER_REGULAR_SUFFIX = "}";
	public final static String SQL_PLACEHOLDER_REGULAR_PARAM = "?";
	
	/**
	 * 数据输出类型
	 */
	public final static String OUTPUT_TARGET_REDIS = "redis";
	public final static String OUTPUT_TARGET_MYSQL = "mysql";
	public final static String OUTPUT_TARGET_ALL = "all";
	
	/**
	 * redis分析数据名称
	 */
	public final static String REDIS_TOPIC_TICKET = "ticket-amount";
	public final static String REDIS_TOPIC_USER_ARCHITECTURE = "user-architecture";
	
	public final static int ANALYSE_DAY_NUM = 2 ;
	
	public final static String REDIS_TMP_CACHENAME_PREFIX_TICKET_PAYMENT = "payment-counter_";
	public final static String REDIS_TMP_CACHENAME_PREFIX_USER_ACTIVE = "active-counter_";
	
	
	
	public final static String REDIS_NAMESPACE_TICKET = "ticket:";
	public final static String REDIS_NAMESPACE_TICKET_PAYMENT = "pay:type:";
	public final static String REDIS_NAMESPACE_USER = "user:";
	public final static String REDIS_NAMESPACE_USER_ACTIVE = "user:active";
	public final static String REDIS_NAMESPACE_USER_ARCHIVITECTURE = "user:offline:architecture:";
	
	
	/**
	 * 支付类型：
	 * 支付宝、微信、中移动、翼支付、首信翼支付、银联、其它
	 */
	public final static String PAYMENT_TYPE_ZIFB = "ZFB";
	public final static String PAYMENT_TYPE_WEIX = "WX";
	public final static String PAYMENT_TYPE_ZHONGYD = "ZYD";
	public final static String PAYMENT_TYPE_YIZF = "YZF";
	public final static String PAYMENT_TYPE_SHOUXYZF = "SXYZF";
	public final static String PAYMENT_TYPE_YINL = "YL";
	public final static String PAYMENT_TYPE_OTHER = "OTHER";
	
	/**
	 * 支付渠道，代码名称映射
	 */
	public static Map<String,String> paymentMaps = new HashMap<String,String>();
	static {
		paymentMaps.put("ZFB", "支付宝");
		paymentMaps.put("WX", "微信");
		paymentMaps.put("ZYD", "中移动");
		paymentMaps.put("YZF", "翼支付");
		paymentMaps.put("SXYZF", "首信易支付");
		paymentMaps.put("YL", "银联");
		paymentMaps.put("OTHER", "其它");
	}
	
	/**
	 * 实时交易记录topic
	 */
	public static final String REDIS_TOPIC_TICKET_RECORD = "ticket:record";
	public static final String REDIS_TOPIC_LAST_TIME = "ticket:lastTime";
	
}
