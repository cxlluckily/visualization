SELECT city_code, days, SUM(num) num 
FROM (
	SELECT city_code, 	
	  order_type, 
	  num,
	  DATE_FORMAT(days, 'yyyy-MM-dd') days 
	FROM (
			select city_code, order_type, ticket_actual_take_ticket_num num, pay_payment_type,TICKET_NOTI_TAKE_TICKET_RESULT_DATE AS days, t_last_timestamp	
			from skp.order_info 
			where order_type='1' and ticket_order_status = '5' 
			and TICKET_NOTI_TAKE_TICKET_RESULT_DATE is not null 
			and TICKET_PICKUP_STATION_CODE is not null 
			and TICKET_ACTUAL_TAKE_TICKET_NUM is not null 
			and pay_payment_type is not null 
			and city_code is not null 
		UNION
			select city_code, order_type, '1' num, pay_payment_type,TOPUP_TOPUP_DATE AS days, t_last_timestamp	
			from skp.order_info 
			where order_type='2' and TOPUP_ORDER_STATUS = '5' 
			and TOPUP_TOPUP_DATE is not null 
			and pay_payment_type is not null 
			and city_code is not null
		UNION
			select city_code, order_type, '1' num, pay_payment_type,XXHF_ORDER_DATE AS days, t_last_timestamp	
			from skp.order_info 
			where order_type = '3' AND XXHF_DEBIT_REQUEST_RESULT = '00'
			and XXHF_ORDER_DATE is not null 
			and pay_payment_type is not null 
			and city_code is not null 
		UNION
			select city_code, order_type, TICKET_ACTUAL_TAKE_TICKET_NUM num, pay_payment_type,TICKET_NOTI_TAKE_TICKET_RESULT_DATE AS days, t_last_timestamp	
			from skp.order_info 
			where order_type = '4' AND ticket_order_status = '5' 
			AND  ORDER_PRODUCT_CODE IN('ZH_RAIL_A','ZH_RAIL_I')
			and TICKET_NOTI_TAKE_TICKET_RESULT_DATE is not null 
			and pay_payment_type is not null 
			and city_code is not null 
		UNION
			SELECT city_code, order_type, '1' num, pay_payment_type, TICKET_NOTI_TAKE_TICKET_RESULT_DATE AS days, t_last_timestamp	
			FROM skp.order_info 
			WHERE order_type = '4' AND ORDER_PRODUCT_CODE NOT IN('ZH_RAIL_A', 'ZH_RAIL_I') 
			AND (xfhx_sjt_status <> 03 AND xfhx_sjt_status <> 99)
			and TICKET_NOTI_TAKE_TICKET_RESULT_DATE is not null 
			and pay_payment_type is not null 
			and city_code is not null 
		UNION
			SELECT city_code, order_type, '1' num, pay_payment_type, COFFEE_UPDATE_DATE AS days, t_last_timestamp	
			FROM skp.order_info 	
			WHERE order_type = '5' AND COFFEE_ORDER_STATE = '4'	
			and COFFEE_UPDATE_DATE is not null 
			and pay_payment_type is not null 
			and city_code is not null 
	) a 
	WHERE 1 = 1 
	<#if startTimestamp??> AND t_last_timestamp >= ${startTimestamp} </#if>
  	<#if stopTimestamp??> AND t_last_timestamp <= ${stopTimestamp} </#if>
) b GROUP BY city_code, days
