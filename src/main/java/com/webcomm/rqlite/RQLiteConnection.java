package com.webcomm.rqlite;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.rqlite.NodeUnavailableException;
import com.webcomm.rqlite.core.CoreDatabaseMetaData;
import com.webcomm.rqlite.core.CorePreparedStatement;
import com.webcomm.rqlite.core.CoreStatement;
import com.webcomm.rqlite.core.DB;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RQLiteConnection implements Connection {

    private final DB db;
    private CoreDatabaseMetaData meta = null;
    private Map<String, Class<?>> typeMap;
    private boolean autoCommit;
    private List<Object> bufferSql;

    public RQLiteConnection(String url) throws SQLException {
        this(url, new Properties());
    }
    
    public RQLiteConnection(String url, Properties prop) throws SQLException {
        this.db = open(url, prop);
        RQLiteConfig config = db.getConfig();

        config.apply(this);
    }

    public DB getDatabase() {
        return db;
    }
    
    private static DB open(String url, Properties props) throws SQLException {
        // Create a copy of the given properties
        Properties newProps = new Properties();
        newProps.putAll(props);

        RQLiteConfig config = new RQLiteConfig(newProps);

        // load the native DB
        DB db = null;
        try {
            db = new DB(url, config);
        }
        catch (Exception e) {
            SQLException err = new SQLException("Error opening connection");
            err.initCause(e);
            throw err;
        }
        db.open();
        return db;
    }

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
        // caller should invoke isWrapperFor prior to unwrap
        return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
	}

	@Override
	public Statement createStatement() throws SQLException {
        checkOpen();
        return new CoreStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        log.debug("prepareStatement(String sql): " + sql);
		return new CorePreparedStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("RQLite does not support Stored Procedures");
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
        return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        log.debug("setAutoCommit: " + autoCommit);
        this.autoCommit = autoCommit;
        this.setBufferSql(new ArrayList<>());
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return autoCommit;
	}

	@Override
	public void commit() throws SQLException {
        checkOpen();
        log.debug("commit... ");
        if (bufferSql.size() > 0) {
        	try {
				db.executeUpdate(bufferSql.toArray(new Object[bufferSql.size()]));
				this.setBufferSql(new ArrayList<>());
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NodeUnavailableException e) {
				e.printStackTrace();
			}
        }
	}

	@Override
	public void rollback() throws SQLException {
		log.debug("try to rollback...");
//        throw new SQLException("RQLite does not support rollback");
	}

	@Override
	public void close() throws SQLException {
		log.debug("connection close...");
        if (isClosed()) return;

        db.close();
	}

	@Override
	public boolean isClosed() throws SQLException {
        return db.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();

        if (meta == null) {
            meta = new CoreDatabaseMetaData(this);
        }

        return meta;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		log.debug("readOnly: " + readOnly);
//        throw new SQLException("Cannot change read-only flag after establishing a connection.");
		
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		log.debug("setCatalog: " + catalog);
        checkOpen();
	}

	@Override
	public String getCatalog() throws SQLException {
        checkOpen();
        return null;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		log.debug("setTransactionIsolation: " + level);
        checkOpen();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
        return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        return new CoreStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		log.debug("prepareStatement(String sql, int resultSetType, int resultSetConcurrency)");
		return prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("RQLite does not support Stored Procedures");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
        synchronized (this) {
            if (this.typeMap == null) {
                this.typeMap = new HashMap<String, Class<?>>();
            }

            return this.typeMap;
        }
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        synchronized (this) {
            this.typeMap = map;
        }
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("RQLite does not support Holdability");
	}

	@Override
	public int getHoldability() throws SQLException {
        throw new SQLException("RQLite does not support Holdability");
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("RQLite does not support Savepoint");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("RQLite does not support Savepoint");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("RQLite does not support rollback");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("RQLite does not support Savepoint");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
        checkOpen();
        log.debug("createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)");
        return new CoreStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		log.debug("prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)");
		return prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
        throw new SQLException("RQLite does not support Stored Procedures");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		log.debug("prepareStatement(String sql, int autoGeneratedKeys)");
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		log.debug("prepareStatement(String sql, int[] columnIndexes)");
		return prepareStatement(sql);
	}

    protected void checkOpen() throws SQLException {
        if (isClosed())
            throw new SQLException("database connection closed");
    }

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkOpen();
        log.debug("prepareStatement(String sql, String[] columnNames)");
		return prepareStatement(sql);
	}

	@Override
	public Clob createClob() throws SQLException {
        // Support this
        throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException {
        // Support this
        throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException {
        // Support this
        throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
        // Support this
        throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }
        Statement statement = createStatement();
        try {
            return statement.execute("select 1");
        } finally {
            statement.close();
        }
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		// Auto-generated method stub
		
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		// Auto-generated method stub
		
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		// Auto-generated method stub
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// Auto-generated method stub
		return null;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		// Auto-generated method stub
		return null;
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("unsupported by RQLite");
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		// Auto-generated method stub
		
	}

	@Override
	public String getSchema() throws SQLException {
		// Auto-generated method stub
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// Auto-generated method stub
		
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		// Auto-generated method stub
		
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// Auto-generated method stub
		return 0;
	}

	public List<Object> getBufferSql() {
		return bufferSql;
	}

	public void setBufferSql(List<Object> bufferSql) {
		this.bufferSql = bufferSql;
	}
}
