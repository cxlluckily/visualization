package com.shankephone.data.monitoring.computing.device.service;

import java.util.List;
import java.util.Map;

import com.shankephone.data.monitoring.computing.device.model.FailureInfo;

public interface FailureInfoService {

	void insert(FailureInfo fi);

	FailureInfo queryById(String cityCode, String deviceId);

	void update(FailureInfo fi);

	Integer deleteByDeviceId(String cityCode, String deviceId, String failureType);

	List<FailureInfo> queryList();

	List<Map<String, Object>> queryDeviceCount();

	Integer delete(FailureInfo fi);

	List<Map<String, Object>> queryFailureTypeCount();

	List<Map<String, Object>> queryStationDeviceCount();

	Map<String,Object> selectPK(String city_code, String device_id, String failure_type);

	/**
	 * 站点、设备汇总
	 * @return
	 */
	List<Map<String, Object>> queryDeviceTypeCount();

	/**
	 * 故障分类汇总
	 * @return
	 */
	List<Map<String, Object>> queryFailureDeviceTypeCount();

	/**
	 * 站点故障汇总
	 * @return
	 */
	List<Map<String, Object>> queryStationDeviceTypeCount();

}
