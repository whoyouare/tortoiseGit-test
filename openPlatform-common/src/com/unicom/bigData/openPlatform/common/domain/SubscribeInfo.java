package com.unicom.bigData.openPlatform.common.domain;

public class SubscribeInfo {

	// 基本信息
	private String subscriptionId;
	private String interfaceId;
	private String developerId;
	private String telNums;// 电话号码
	private String imeis;
	private String tacs;
	private String apps;
	private String searchKeys;
	private String tags;// 用户标签
	// private String context;// 上下文
	private String period;// day
	private String terminalBrands;// 终端品牌
	private String terminalTypes;// 终端品牌

	// 区域相关
	private String areaIds;
	private String areaThreshold;
	private String permanentRes;// 常住地
	private String localeRes;// 归属地
	private String workRes;// 工作地
	// TODO:status，新加订阅服务时是否设置成有效，如果设置成有效需要修改订阅接口文档，添加status参数
	private String status;
	private String subTime;
	private String upTime;

	// public String getContext() {
	// return context;
	// }
	//
	// public void setContext(String context) {
	// this.context = context;
	// }

	public String getPermanentRes() {
		return permanentRes;
	}

	public void setPermanentRes(String permanentRes) {
		this.permanentRes = permanentRes;
	}

	public String getLocaleRes() {
		return localeRes;
	}

	public void setLocaleRes(String localeRes) {
		this.localeRes = localeRes;
	}

	public String getWorkRes() {
		return workRes;
	}

	public void setWorkRes(String workRes) {
		this.workRes = workRes;
	}

	public String getSubscriptionId() {
		return subscriptionId;
	}

	public void setSubscriptionId(String subscriptionId) {
		this.subscriptionId = subscriptionId;
	}

	public String getInterfaceId() {
		return interfaceId;
	}

	public void setInterfaceId(String interfaceId) {
		this.interfaceId = interfaceId;
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}

	public String getAreaThreshold() {
		return areaThreshold;
	}

	public void setAreaThreshold(String areaThreshold) {
		this.areaThreshold = areaThreshold;
	}

	public String getSubTime() {
		return subTime;
	}

	public void setSubTime(String subTime) {
		this.subTime = subTime;
	}

	public String getUpTime() {
		return upTime;
	}

	public void setUpTime(String upTime) {
		this.upTime = upTime;
	}

	public String getImeis() {
		return imeis;
	}

	public void setImeis(String imeis) {
		this.imeis = imeis;
	}

	public String getTacs() {
		return tacs;
	}

	public void setTacs(String tacs) {
		this.tacs = tacs;
	}

	public String getAreaIds() {
		return areaIds;
	}

	public void setAreaIds(String areaIds) {
		this.areaIds = areaIds;
	}

	public String getDeveloperId() {
		return developerId;
	}

	public void setDeveloperId(String developerId) {
		this.developerId = developerId;
	}

	public String getTelNums() {
		return telNums;
	}

	public void setTelNums(String telNums) {
		this.telNums = telNums;
	}

	public String getApps() {
		return apps;
	}

	public void setApps(String apps) {
		this.apps = apps;
	}

	public String getSearchKeys() {
		return searchKeys;
	}

	public void setSearchKeys(String searchKeys) {
		this.searchKeys = searchKeys;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public String getTerminalBrands() {
		return terminalBrands;
	}

	public void setTerminalBrands(String terminalBrands) {
		this.terminalBrands = terminalBrands;
	}

	public String getTerminalTypes() {
		return terminalTypes;
	}

	public void setTerminalTypes(String terminalTypes) {
		this.terminalTypes = terminalTypes;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

}
