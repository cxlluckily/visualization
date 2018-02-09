package com.shankephone.hive;

import java.io.Serializable;

public class OrderInfo implements Serializable {
	
	private String order_no;
	
	private int order_type;
	
	private String city_code;

	public String getOrder_no() {
		return order_no;
	}

	public void setOrder_no(String order_no) {
		this.order_no = order_no;
	}

	public int getOrder_type() {
		return order_type;
	}

	public void setOrder_type(int order_type) {
		this.order_type = order_type;
	}

	public String getCity_code() {
		return city_code;
	}

	public void setCity_code(String city_code) {
		this.city_code = city_code;
	}

	@Override
	public String toString() {
		return "OrderInfo [order_no=" + order_no + ", order_type=" + order_type + ", city_code=" + city_code + "]";
	}
	
}
