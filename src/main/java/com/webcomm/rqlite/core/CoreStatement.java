package com.webcomm.rqlite.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.rqlite.NodeUnavailableException;
import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.QueryResults;
import com.webcomm.rqlite.RQLiteConnection;

public class CoreStatement implements Statement, Codes {

    public final RQLiteConnection conn;
    protected String     sql            = null;
    protected Object[]   batch          = null;
    protected CoreResultSet   rs;

    public CoreStatement(RQLiteConnection c) {
        conn = c;
		System.out.println("CoreStatement");
        try {
			rs = new CoreResultSet(this);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		try {
			System.out.println("CoreStatement executeQuery sql : " + sql);
			
			QueryResults results = conn.getDatabase().executeQuery(sql);
			
			CoreResultSet resultSet = new CoreResultSet(this, results);

			return resultSet;
		} catch (SQLException e) {
			throw e;
		} catch (NodeUnavailableException e) {
			throw new SQLException("cannot connect to a rqlite node");
		}
	}

	public ResultSet executeQuery(Object[] sql) throws SQLException {
		try {
			Gson gson = new Gson();
			System.out.println("CoreStatement executeQuery :" + gson.toJson(sql));
			
			QueryResults results = conn.getDatabase().executeQuery(sql);
			
			CoreResultSet resultSet = new CoreResultSet(this, results);

			return resultSet;
		} catch (SQLException e) {
			throw e;
		} catch (NodeUnavailableException e) {
			throw new SQLException("cannot connect to a rqlite node");
		}
	}
	
	@Override
	public int executeUpdate(String sql) throws SQLException {
		try {
			System.out.println("CoreStatement executeUpdate sql : " + sql);
			
			ExecuteResults results = conn.getDatabase().executeUpdate(sql);

			return checkResult(results);
		} catch (SQLException e) {
			throw e;
		} catch (NodeUnavailableException e) {
			throw new SQLException("cannot connect to a rqlite node");
		}
	}

	public int executeUpdate(Object[] sql) throws SQLException {
		try {
			Gson gson = new Gson();
			System.out.println("CoreStatement executeUpdate :" + gson.toJson(sql));
			
			ExecuteResults results = conn.getDatabase().executeUpdate(sql);

			return checkResult(results);
		} catch (SQLException e) {
			throw e;
		} catch (NodeUnavailableException e) {
			throw new SQLException("cannot connect to a rqlite node");
		}
	}
	
	private int checkResult(ExecuteResults results) throws SQLException {

		Gson gson = new Gson();
		System.out.println("CoreStatement checkResult ExecuteResults " + gson.toJson(results));
		
		ExecuteResults.Result[] resultArray = results.results;
		if (resultArray.length > 0) {
			ExecuteResults.Result result = resultArray[0];
			
			if(result != null) {

				if(StringUtils.isNotBlank(result.error)){
					throw new SQLException(result.error);
				}
				
				return result.rowsAffected;
			}
		}
		
		return SQLITE_OK;
	}

	@Override
	public void close() throws SQLException {
		
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public int getMaxRows() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		// Done Not Do : Ted S1
	}

	@Override
	public void cancel() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		System.out.println("execute sql : " + sql);
		
		ResultSet result = executeQuery(sql);
		if(result != null) {
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		// Done Not Do : Ted S2
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public int[] executeBatch() throws SQLException {
		if(batch != null) {
			Gson gson = new Gson();
			Object[] query = new Object[1];
			query[0] = batch;
			System.out.println("CoreStatement executeBatch :" + gson.toJson(query));
			
			return new int[] {executeUpdate(query)};
		}
		else {
			System.out.println("CoreStatement executeBatch : " + sql);
			executeUpdate(sql);
			return new int[] {executeUpdate(sql)};
		}
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return executeUpdate(sql);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}
}
