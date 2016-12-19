package com.unicom.bigData.openPlatform.common.inceptor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class InceptorConnManager implements IDBConnectionManager {

	private DbConfig config = null;

	public DbConfig getConfig() {
		return config;
	}

	public void setConfig(DbConfig config) {
		this.config = config;
	}

	public InceptorConnManager(DbConfig dbConfig) {
		this.config = dbConfig;
	}

	// build non-thread safe connection mode, to reduce the connection cost.
	Connection conn = null;

	public Connection getConnection() {
		if (conn != null) {
			return conn;
		}
		if (retryCnt < MaxRetryCnt) {

		}
		try {
			Thread.sleep(1000);
			Class.forName(config.getDriverClass());
			// jdbc:hive2://bzSysAi-backend-online001-bjdxt9.qiyi.virtual:10000/default
			conn = DriverManager.getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
			return conn;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}

	int retryCnt = 0;
	static final int MaxRetryCnt = 50;

	public Object executeQuery(String sql, ResultSetHandler handler) {

		Statement stmt = null;
		ResultSet rs = null;
		try {
			boolean tryConnect = true;
			while (tryConnect) {
				try {
					getConnection();
					stmt = conn.createStatement();
					// System.out.println(sql);
					rs = stmt.executeQuery(sql);
					retryCnt = 0;
					break;
				} catch (Exception e) {
					retryCnt++;
					e.printStackTrace();
					if (stmt != null) {
						try {
							stmt.close();
						} catch (Exception e1) {
							System.out.println("Close statement fails: " + e1.getMessage());
						}
					}
					if (conn != null) {
						try {
							conn.close();
						} catch (Exception e2) {
							System.out.println("Close connection fails: " + e2.getMessage());
						}
						conn = null;
					}
					System.out.println("Net connection exception occurs: " + e.getMessage()
							+ ", and will retry the connection ");
				}
			}
			return handler.handleResultSet(rs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (false) {
					// use the long connection
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public void executeUpdate(String sql) {

		Statement stmt = null;
		try {
			boolean tryConnect = true;
			while (tryConnect) {
				try {
					getConnection();
					stmt = conn.createStatement();
					// System.out.println(sql);
					stmt.executeUpdate(sql);
					retryCnt = 0;
					conn.commit();
					break;
				} catch (Exception e) {
					retryCnt++;
					e.printStackTrace();
					if (stmt != null) {
						try {
							stmt.close();
						} catch (Exception e1) {
							System.out.println("Close statement fails: " + e1.getMessage());
						}
					}
					if (conn != null) {
						try {
							conn.close();
						} catch (Exception e2) {
							System.out.println("Close connection fails: " + e2.getMessage());
						}
						conn = null;
					}
					System.out.println("Net connection exception occurs: " + e.getMessage()
							+ ", and will retry the connection ");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (false) {
					// use the long connection
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void execute(String sql) {

		Statement stmt = null;
		try {
			boolean tryConnect = true;
			while (tryConnect) {
				try {
					getConnection();
					stmt = conn.createStatement();
					// System.out.println(sql);
					stmt.execute(sql);
					retryCnt = 0;
					break;
				} catch (Exception e) {
					retryCnt++;
					e.printStackTrace();
					if (stmt != null) {
						try {
							stmt.close();
						} catch (Exception e1) {
							System.out.println("Close statement fails: " + e1.getMessage());
						}
					}
					if (conn != null) {
						try {
							conn.close();
						} catch (Exception e2) {
							System.out.println("Close connection fails: " + e2.getMessage());
						}
						conn = null;
					}
					System.out.println("Net connection exception occurs: " + e.getMessage()
							+ ", and will retry the connection ");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (false) {
					// use the long connection
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public <E> Object executeUpdateForLastInserId(String sql, PrepareStatementParameterHandler parameterHandler,
			ResultSetHandler handler, E parameterObj) {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		String idQuerySql = "SELECT LAST_INSERT_ID() as ID";
		try {
			boolean tryConnect = true;
			while (tryConnect) {
				try {
					getConnection();
					stmt = conn.prepareStatement(sql);
					parameterHandler.handleParameterSet(stmt, parameterObj);
					// System.out.println(sql);
					stmt.executeUpdate();
					rs = stmt.executeQuery(idQuerySql);
					retryCnt = 0;
					break;
				} catch (Exception e) {
					retryCnt++;
					e.printStackTrace();
					if (stmt != null) {
						try {
							stmt.close();
						} catch (Exception e1) {
							System.out.println("Close statement fails: " + e1.getMessage());
						}
					}
					if (conn != null) {
						try {
							conn.close();
						} catch (Exception e2) {
							System.out.println("Close connection fails: " + e2.getMessage());
						}
						conn = null;
					}
					System.out.println("Net connection exception occurs: " + e.getMessage()
							+ ", and will retry the connection ");
				}
			}
			return handler.handleResultSet(rs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (false) {
					// use the long connection
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public <E> void batchExecute(String sql, List<E> dataList, PrepareStatementHandler handler) {
		PreparedStatement stmt = null;
		try {
			boolean tryConnect = true;
			while (tryConnect) {
				try {
					getConnection();
					retryCnt = 0;
					break;
				} catch (Exception e) {
					retryCnt++;
					if (conn != null) {
						try {
							conn.close();
						} catch (Exception e2) {
							System.out.println("Close connection fails: " + e2.getMessage());
						}
						conn = null;
					}
					System.out.println("Net connection exception occurs: " + e.getMessage()
							+ ", and will retry the connection ");
				}
			}

			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(sql);
			try {
				handler.prepareStatementAddBatch(stmt, dataList);
			} catch (Exception e) {
				e.printStackTrace();
			}
			stmt.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (false) {
					// use the long connection
					conn.close();
				}
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
