select city_code,pay_pay_account
from skp.order_info
where order_type=1 
and ticket_order_status=5
and pay_pay_account is not null
and pay_pay_time is not null
and city_code is not null
<#if current_date??> and DATE_FORMAT(pay_pay_time, 'yyyy-MM-dd') = ${current_date} </#if>
<#if stopTimestamp??> and t_last_timestamp<=${stopTimestamp} </#if>