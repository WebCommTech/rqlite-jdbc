package com.webcomm.rqlite.core;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.webcomm.rqlite.RQLiteConnection;

public class CorePreparedStatement extends CoreStatement implements PreparedStatement, ParameterMetaData {

    protected int paramCount;
    protected int        batchPos;
    
	public CorePreparedStatement(RQLiteConnection c, String sql) {
		super(c);

		Gson gson = new Gson();
		System.out.println("CorePreparedStatement sql " + gson.toJson(sql));
        this.sql = sql;
        setParamCount();
	}
	
	private void setParamCount() {

	    int count = 0;
	    Pattern patternObject = Pattern.compile("\\?");
	    Matcher matcher = patternObject.matcher(sql);
	    while(matcher.find()){
	        count++;
	    }
	    paramCount = count;
		System.out.println("setParamCount : " + count);
	}
	
    protected void batch(int pos, String value) throws SQLException {
		Gson gson = new Gson();
		System.out.println("pos " + gson.toJson(pos));
		System.out.println("value " + gson.toJson(value));
        if (batch == null) {
            batch = new String[paramCount+1];
            batch[0] = sql;
            batchPos = 1;
        }
        batch[batchPos + pos - 1] = value;
    }

	@Override
	public int getParameterCount() throws SQLException {
		throw new SQLException("not implement");
//		return 0;
	}

	@Override
	public int isNullable(int param) throws SQLException {
		throw new SQLException("not implement");
//		return 0;
	}

	@Override
	public boolean isSigned(int param) throws SQLException {
		throw new SQLException("not implement");
//		return false;
	}

	@Override
	public int getPrecision(int param) throws SQLException {
		throw new SQLException("not implement");
//		return 0;
	}

	@Override
	public int getScale(int param) throws SQLException {
		throw new SQLException("not implement");
//		return 0;
	}

	@Override
	public int getParameterType(int param) throws SQLException {
		throw new SQLException("not implement");
//		return 0;
	}

	@Override
	public String getParameterTypeName(int param) throws SQLException {
		throw new SQLException("not implement");
//		return null;
	}

	@Override
	public String getParameterClassName(int param) throws SQLException {
		throw new SQLException("not implement");
//		return null;
	}

	@Override
	public int getParameterMode(int param) throws SQLException {
		throw new SQLException("not implement");
//		return 0;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		if(batch != null) {
			Gson gson = new Gson();
			Object[] query = new Object[1];
			query[0] = batch;
			System.out.println("CorePreparedStatement executeQuery :" + gson.toJson(query));
			return executeQuery(query);
		}
		else {
			System.out.println("CorePreparedStatement executeQuery : " + sql);
			return executeQuery(sql);
		}
	}

	@Override
	public int executeUpdate() throws SQLException {
		if(batch != null) {
			Gson gson = new Gson();
			Object[] query = new Object[1];
			query[0] = batch;
			System.out.println("CorePreparedStatement executeUpdate :" + gson.toJson(query));
			return executeUpdate(query);
		}
		else {
			System.out.println("CorePreparedStatement executeUpdate : " + sql);
			return executeUpdate(sql);
		}
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
        batch(parameterIndex, "NULL"); // TODO
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setInt(parameterIndex, x ? 1 : 0);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
        batch(parameterIndex, new Short(x).toString());
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
        batch(parameterIndex, new Integer(x).toString());
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
        batch(parameterIndex, new Long(x).toString());
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
        batch(parameterIndex, new Float(x).toString());
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
        batch(parameterIndex, new Double(x).toString());
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void clearParameters() throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		Gson gson = new Gson();
		System.out.println("setObject " + gson.toJson(x));
		
        if (x == null) {
            batch(parameterIndex, null);
        }
        else if (x instanceof java.util.Date) {
            setDateByMilliseconds(parameterIndex, ((java.util.Date) x).getTime());
        }
        else if (x instanceof Date) {
            setDateByMilliseconds(parameterIndex, new Long(((Date) x).getTime()));
        }
        else if (x instanceof Time) {
            setDateByMilliseconds(parameterIndex, new Long(((Time) x).getTime()));
        }
        else if (x instanceof Timestamp) {
            setDateByMilliseconds(parameterIndex, new Long(((Timestamp) x).getTime()));
        }
        else if (x instanceof Long) {
            batch(parameterIndex, x.toString());
        }
        else if (x instanceof Integer) {
            batch(parameterIndex, x.toString());
        }
        else if (x instanceof Short) {
            batch(parameterIndex, new Integer(((Short) x).intValue()).toString());
        }
        else if (x instanceof Float) {
            batch(parameterIndex, x.toString());
        }
        else if (x instanceof Double) {
            batch(parameterIndex, x.toString());
        }
        else if (x instanceof Boolean) {
            setBoolean(parameterIndex, ((Boolean) x).booleanValue());
        }
        else if (x instanceof byte[]) {
            batch(parameterIndex, x.toString());
        }
        else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal)x);
        }
        else {
            batch(parameterIndex, x.toString());
        }
	}
	
    /**
    * Store the date in the user's preferred format (text, int, or real)
    */
   protected void setDateByMilliseconds(int pos, Long value) throws SQLException {
		System.out.println("setDateByMilliseconds " + value);
		System.out.println("setDateByMilliseconds " + conn.getDatabase().getConfig().dateMultiplier);
		
       switch(conn.getDatabase().getConfig().dateClass) {

           default: //INTEGER:
               batch(pos, new Long(value / conn.getDatabase().getConfig().dateMultiplier).toString());
       }
   }

	@Override
	public boolean execute() throws SQLException {
		throw new SQLException("not implement");
//		return false;
	}

	@Override
	public void addBatch() throws SQLException {
		// Done Not Do : Ted S3
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLException("not implement");
//		return null;
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new SQLException("not implement");
//		return null;
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLException("not implement");
		
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException("not implement");
		
	}

}
