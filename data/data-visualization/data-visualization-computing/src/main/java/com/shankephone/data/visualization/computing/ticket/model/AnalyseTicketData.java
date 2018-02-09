package com.shankephone.data.visualization.computing.ticket.model;

import java.util.Date;

import com.shankephone.data.visualization.computing.common.model.BaseModel;

/**
 * 按维度统计数据
 * @author fengql
 * @version 2017年6月12日 上午10:00:48
 */
public class AnalyseTicketData extends BaseModel{
	
	private static final long serialVersionUID = 1L;
	
	private Long id					;
	//维度类型
	private String dimension_type			;
	//维度代码
	private String dimension_code			;
	//维度名称
	private String dimension_name;
	//统计时段类型（0-小时，1-天，2-周，2-月，4-年）
	private Integer period_type		;
	//开始时间
	private String begin_time			;
	//结束时间
	private String end_time			    ;
	//订单数量 
	private Integer order_num			;
	//销售总票数
	private Integer total_num			;
	//销售总金额
	private Double total_amount		    ;
	private Date create_time;
	private Date update_time;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getDimension_type() {
		return dimension_type;
	}
	public void setDimension_type(String dimension_type) {
		this.dimension_type = dimension_type;
	}
	public String getDimension_code() {
		return dimension_code;
	}
	public void setDimension_code(String dimension_code) {
		this.dimension_code = dimension_code;
	}
	public String getDimension_name() {
		return dimension_name;
	}
	public void setDimension_name(String dimension_name) {
		this.dimension_name = dimension_name;
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
	public Integer getOrder_num() {
		return order_num;
	}
	public void setOrder_num(Integer order_num) {
		this.order_num = order_num;
	}
	public Integer getTotal_num() {
		return total_num;
	}
	public void setTotal_num(Integer total_num) {
		this.total_num = total_num;
	}
	public Double getTotal_amount() {
		return total_amount;
	}
	public void setTotal_amount(Double total_amount) {
		this.total_amount = total_amount;
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
