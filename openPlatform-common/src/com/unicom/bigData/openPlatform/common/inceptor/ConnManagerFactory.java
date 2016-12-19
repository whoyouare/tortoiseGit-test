package com.unicom.bigData.openPlatform.common.inceptor;

import com.unicom.bigData.openPlatform.common.SpringContextUtil;


public class ConnManagerFactory {

	private static InceptorConnManager openPlatformConnManager = null;

	static {
		openPlatformConnManager = getConnManager("dbCf-openPlatform");
	}

	private static InceptorConnManager getConnManager(String dbCfKey) {
		DbConfig config = (DbConfig) SpringContextUtil.getApplicationContext().getBean(dbCfKey);
		return new InceptorConnManager(config);
	}

	public static InceptorConnManager getOpenPlatformConnManager() {
		return openPlatformConnManager;
	}

}
