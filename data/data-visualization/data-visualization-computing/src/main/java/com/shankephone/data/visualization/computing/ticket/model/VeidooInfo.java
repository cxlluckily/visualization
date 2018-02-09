package com.shankephone.data.visualization.computing.ticket.model;

import com.shankephone.data.visualization.computing.common.model.BaseModel;

/**
 * 维度关系信息
 * @author fengql
 * @version 2017年6月12日 上午10:14:55
 */
public class VeidooInfo extends BaseModel {
	private static final long serialVersionUID = 1L;
	private Long id			;	
	private String city_code	;	
	private String city_name	;	
	private String line_code	;	
	private String line_name	;	
	private String station_code	;
	private String station_name	;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getCity_code() {
		return city_code;
	}
	public void setCity_code(String city_code) {
		this.city_code = city_code;
	}
	public String getCity_name() {
		return city_name;
	}
	public void setCity_name(String city_name) {
		this.city_name = city_name;
	}
	public String getLine_code() {
		return line_code;
	}
	public void setLine_code(String line_code) {
		this.line_code = line_code;
	}
	public String getLine_name() {
		return line_name;
	}
	public void setLine_name(String line_name) {
		this.line_name = line_name;
	}
	public String getStation_code() {
		return station_code;
	}
	public void setStation_code(String station_code) {
		this.station_code = station_code;
	}
	public String getStation_name() {
		return station_name;
	}
	public void setStation_name(String station_name) {
		this.station_name = station_name;
	}
	
	

}
