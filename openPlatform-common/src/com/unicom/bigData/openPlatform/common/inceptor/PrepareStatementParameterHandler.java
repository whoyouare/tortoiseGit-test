package com.unicom.bigData.openPlatform.common.inceptor;

import java.sql.PreparedStatement;

public interface PrepareStatementParameterHandler<P> {

	public  void handleParameterSet(PreparedStatement ps, P parameterObj) throws Exception ;
	
}
