package com.unicom.bigData.openPlatform.common.inceptor;

import java.sql.Connection;
import java.util.List;

public interface IDBConnectionManager {

	public Connection getConnection();

	public Object executeQuery(String sql, ResultSetHandler handler);

	public void executeUpdate(String sql);

	public void execute(String sql);

	public <E> Object executeUpdateForLastInserId(String sql, PrepareStatementParameterHandler parameterHandler,
			ResultSetHandler resultHandler, E parameterObj);

	<E> void batchExecute(String sql, List<E> dataList, PrepareStatementHandler handler);

}
