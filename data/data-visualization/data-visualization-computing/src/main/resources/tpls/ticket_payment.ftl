SELECT city_code, days, payment_code, SUM(num) num FROM (
	SELECT city_code, 	
	  order_type, 
	  CASE WHEN pay_payment_type = 0 OR pay_payment_type = 2 OR pay_payment_type = 6 THEN 'ZFB'
		ELSE 
			CASE WHEN pay_payment_type = 1 OR pay_payment_type = 9 THEN 'ZYD' 
		ELSE
			CASE WHEN pay_payment_type = 3 OR pay_payment_type = 4 OR pay_payment_type = 7 
			OR pay_payment_type = 12 OR pay_payment_type = 13 OR pay_payment_type = 14 THEN 'WX' 
		ELSE 
			CASE WHEN pay_payment_type = 5 THEN 'YZF' 
		ELSE 
			CASE WHEN pay_payment_type = 8 THEN 'SXYZF' 
		ELSE 
			CASE WHEN pay_payment_type = 11 OR pay_payment_type = 'YLSF' THEN 'YL' 
		ELSE 'OTHER' 
	    END END END END END
	  END payment_code,	
	  num,
	  DATE_FORMAT(days, 'yyyy-MM-dd') days 
	 FROM (	
		SELECT city_code, order_type, ticket_actual_take_ticket_num num, pay_payment_type, 
			TICKET_NOTI_TAKE_TICKET_RESULT_DATE
			AS days, t_last_timestamp	
		FROM skp.order_info o	
		WHERE ticket_order_status = '5' AND order_type = '1'
		UNION 
		SELECT city_code, order_type, '1' num, pay_payment_type, 
			TOPUP_TOPUP_DATE AS days, t_last_timestamp	
		FROM skp.order_info o	
		WHERE ticket_order_status = '5' AND order_type = '2'
		UNION 
		SELECT city_code, order_type, '1' num, pay_payment_type, 		
				 XXHF_ORDER_DATE AS days, t_last_timestamp	
			FROM skp.order_info o	
			WHERE 
			order_type = '3' AND XXHF_DEBIT_REQUEST_RESULT = '00'
		UNION 
		SELECT city_code, order_type, 
			TICKET_ACTUAL_TAKE_TICKET_NUM num, pay_payment_type, 			
			XFHX_SJT_SALE_DATE 
				 AS days, t_last_timestamp	
			FROM skp.order_info o	
		WHERE order_type = '4' AND  ORDER_PRODUCT_CODE IN('ZH_RAIL_A','ZH_RAIL_I') AND ticket_order_status = '5' 
		UNION
		SELECT city_code, order_type, '1' num, pay_payment_type, 			
			TICKET_NOTI_TAKE_TICKET_RESULT_DATE 
				 AS days, t_last_timestamp	
			FROM skp.order_info o	
			WHERE order_type = '4' AND
			ORDER_PRODUCT_CODE NOT IN('ZH_RAIL_A', 'ZH_RAIL_I') AND (xfhx_sjt_status <> 03 AND xfhx_sjt_status <> 99)
		UNION 
		SELECT city_code, order_type, '1' num, pay_payment_type, 
				 COFFEE_UPDATE_DATE AS days, t_last_timestamp	
			FROM skp.order_info o	
			WHERE order_type = '5' AND COFFEE_ORDER_STATE = '4'		
		
		) a 
	WHERE 1 = 1 
	<#if lastTime?? >AND t_last_timestamp <= ${lastTime}</#if>
) b GROUP BY city_code, days, payment_code