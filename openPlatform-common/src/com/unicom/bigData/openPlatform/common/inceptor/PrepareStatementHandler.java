package com.unicom.bigData.openPlatform.common.inceptor;

import java.sql.PreparedStatement;
import java.util.List;

public interface PrepareStatementHandler {
	
	public  void prepareStatementAddBatch(PreparedStatement stmt,List datas) throws Exception ;
}
