select city_code, days, count(pay_account) from (

		select t.city_code, t.days, t.pay_account from (
			SELECT t1.*, row_number() over (PARTITION BY t1.CITY_CODE, t1.PAY_ACCOUNT, t1.days ORDER BY t1.days) AS rn FROM (
				SELECT  CITY_CODE, 
					PAY_PAY_ACCOUNT PAY_ACCOUNT,
				 	DATE_FORMAT(PAY_PAY_TIME, 'yyyy-MM-dd') days 
				FROM skp.order_info
				WHERE 
					(((PAY_REFUND_STATE = 0 OR PAY_REFUND_STATE = 1)
					AND TICKET_ORDER_STATUS = 5) or (pay_payment_type = 'YLSF'))
					AND pay_pay_time is not null
					AND city_code is not null
					<#if lastTime??> AND t_last_timestamp <= ${lastTime} </#if>
			) t1 		
		) t where t.rn = 1 <#if startDay??> and days >= ${startDay} </#if> <#if endDay??> AND days <= ${endDay} </#if>
	
) b group by city_code, days