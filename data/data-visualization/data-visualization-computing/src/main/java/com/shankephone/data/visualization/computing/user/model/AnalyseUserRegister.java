package com.shankephone.data.visualization.computing.user.model;

import java.util.Date;

import com.shankephone.data.visualization.computing.common.model.BaseModel;

/**
 * 用户体系表注册统计信息
 * @author fengql
 * @version 2017年7月10日 下午4:23:05
 */
public class AnalyseUserRegister extends BaseModel{
	
	private static final long serialVersionUID = 1L;
	
	private Long id;
	//区域代码
	private String region_code;
	//维度类型
	private String parent_category;
	//维度代码
	private String category;
	//统计时段类型（0-天，1-周，2-月，3-年）
	private Integer period_type ;
	//开始时间
	private String begin_time ;
	//结束时间
	private String end_time ;
	//订单数量 
	private Integer register_num ;
	//创建时间
	private Date create_time;
	//修改时间
	private Date update_time;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	
	public String getRegion_code() {
		return region_code;
	}
	public void setRegion_code(String region_code) {
		this.region_code = region_code;
	}
	public String getParent_category() {
		return parent_category;
	}
	public void setParent_category(String parent_category) {
		this.parent_category = parent_category;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public Integer getPeriod_type() {
		return period_type;
	}
	public void setPeriod_type(Integer period_type) {
		this.period_type = period_type;
	}
	public String getBegin_time() {
		return begin_time;
	}
	public void setBegin_time(String begin_time) {
		this.begin_time = begin_time;
	}
	public String getEnd_time() {
		return end_time;
	}
	public void setEnd_time(String end_time) {
		this.end_time = end_time;
	}
	public Integer getRegister_num() {
		return register_num;
	}
	public void setRegister_num(Integer register_num) {
		this.register_num = register_num;
	}
	public Date getCreate_time() {
		return create_time;
	}
	public void setCreate_time(Date create_time) {
		this.create_time = create_time;
	}
	public Date getUpdate_time() {
		return update_time;
	}
	public void setUpdate_time(Date update_time) {
		this.update_time = update_time;
	}
	
	

}
