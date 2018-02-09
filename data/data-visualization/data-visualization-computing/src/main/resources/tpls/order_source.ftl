select days, sum(skp_count) skp_count,
	sum(skp_wechat_count) skp_wechat_count,
	sum(track_wechatsub_count) track_count,
	sum(ali_cityservice_count) ali_count,
	sum(hb_cityservice_count) hb_count,
	sum(cloud_machine_count) cloud_count,
	sum(esurfing_count) esurfing_count
from (
	SELECT 
		DATE_FORMAT(ticket_noti_take_ticket_result_date, 'yyyy-MM-dd') days,
	    CASE WHEN order_order_source IN ('00', '01') 
	      THEN 1 ELSE 0 
	    END skp_count,
	    CASE WHEN pay_payment_type = '7' AND pay_source = '5' 
	      THEN 1 ELSE 0 
	    END skp_wechat_count,
	    CASE WHEN pay_payment_type = '7' AND pay_source = '6' 
	      THEN 1 ELSE 0 
	    END track_wechatsub_count,
	    CASE WHEN pay_payment_type = '6' 
	      THEN 1 ELSE 0 
	    END ali_cityservice_count,
	    CASE WHEN pay_payment_type = '9' 
	      THEN 1 ELSE 0 
	    END hb_cityservice_count,
	    CASE WHEN  pay_payment_type = 2 OR pay_payment_type = 4 OR order_order_source = '02'
	      THEN 1 ELSE 0 
	    END cloud_machine_count,
	    CASE WHEN pay_payment_type = '5' 
	      THEN 1 ELSE 0 
	    END esurfing_count 
	FROM skp.order_info
	WHERE order_type = 1 
		AND ticket_order_status = 5
		<#if lastTime?? > AND t_last_timestamp <= ${lastTime}</#if>
  ) b <#if day??> where days = ${day}</#if>
GROUP BY days 