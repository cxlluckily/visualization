package com.shankephone.data.visualization.computing.ticket.model;

import com.shankephone.data.visualization.computing.common.model.BaseModel;

/**
 * 城市代码参照
 * @author fengql
 * @version 2017年6月12日 上午9:54:11
 */
public class CityCodeMapping extends BaseModel{
	
	private static final long serialVersionUID = 1L;
	
	private Long id         ;
	private String city_code  ;
	private String city_name  ;
	//mcwlt库中城市代码的值
	private String mcwlt_code ;
	//omp库中城市代码的值
	private String omp_codeva ;
	
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
	public String getMcwlt_code() {
		return mcwlt_code;
	}
	public void setMcwlt_code(String mcwlt_code) {
		this.mcwlt_code = mcwlt_code;
	}
	public String getOmp_codeva() {
		return omp_codeva;
	}
	public void setOmp_codeva(String omp_codeva) {
		this.omp_codeva = omp_codeva;
	}
	
	

}
