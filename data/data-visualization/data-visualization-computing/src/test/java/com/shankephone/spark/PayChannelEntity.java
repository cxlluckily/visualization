package com.shankephone.spark;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 森
 * @version 2017年6月13日 下午4:48:45
 */
public class PayChannelEntity {
	private String order_no;					//订单号
	private String partner_no;				//商户号
	private String payment_vendor;		//支付渠道
	private Double total_amount;		//总金额
	private String pay_time;					//支付时间
	

	public String getOrder_no() {
		return order_no;
	}
	public void setOrder_no(String order_no) {
		this.order_no = order_no;
	}
	public String getPartner_no() {
		return partner_no;
	}
	public void setPartner_no(String partner_no) {
		this.partner_no = partner_no;
	}
	public String getPayment_vendor() {
		return payment_vendor;
	}
	public void setPayment_vendor(String payment_vendor) {
		this.payment_vendor = payment_vendor;
	}
	public Double getTotal_amount() {
		return total_amount;
	}
	public void setTotal_amount(Double total_amount) {
		this.total_amount = total_amount;
	}
	public String getPay_time() {
		return pay_time;
	}
	public void setPay_time(Date pay_time) {
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		 this.pay_time = sdf.format(pay_time);
	}

}
