package com.webcomm.rqlite.core;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.rqlite.dto.QueryResults;
import com.webcomm.rqlite.RQLiteConnection;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoreResultSet implements ResultSet, ResultSetMetaData {

	protected final CoreStatement stmt;
	protected final RQLiteConnection conn;
	
	protected QueryResults results;
	protected QueryResults.Result queryResultsResult;

	public boolean open = false; // true means have results and can iterate them
	protected int row = -1; // number of current row, starts at 1 (0 is for before loading data)
	protected int lastCol; // last column accessed, for wasNull(). -1 if none
	public int maxRows; // max. number of rows as set by a Statement
	public String[] colsMeta = null;
    protected Map<String, Integer> columnNameToIndex = null;
    public String[]           cols     = null; // if null, the RS is closed()
    
    // QueryResults
    public String error;
    public Object[][] values;
	Gson gson = new Gson();

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	public CoreResultSet(CoreStatement stmt) throws SQLException {
		this.stmt = stmt;
		this.conn = (RQLiteConnection) stmt.getConnection();
		
	}

	public CoreResultSet(CoreStatement stmt, QueryResults results) throws SQLException {
		log.debug("results " + gson.toJson(results));
		
		sdf.setTimeZone(TimeZone.getTimeZone("GMT") );
		
		this.stmt = stmt;
		this.conn = (RQLiteConnection) stmt.getConnection();
		this.results = results;
		
		stmt.setResultSet(this);
		
		if(results != null) {

			QueryResults.Result[] resultArray = results.results;
			if (resultArray.length > 0) {
				this.queryResultsResult = resultArray[0];
				
				if(queryResultsResult != null) {
					open = true;
	
					cols = this.queryResultsResult.columns;
					log.debug("cols " + gson.toJson(cols));
					colsMeta = this.queryResultsResult.types;
					log.debug("colsMeta " + gson.toJson(colsMeta));
					
					values = this.queryResultsResult.values;
					log.debug("values " + gson.toJson(values));
					
					if(values != null) {
						maxRows = values.length;
						log.debug("maxRows " + maxRows);
					}
					
					error = queryResultsResult.error;
					
					if(StringUtils.isNotBlank(error)) {
						throw new SQLException(error);
					}
				}
			} else {
				this.queryResultsResult = null;
			}
		}
		else {
			this.queryResultsResult = null;
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	@Override
	public int getColumnCount() throws SQLException {
		if (colsMeta != null) {
			return colsMeta.length;
		}
		return 0;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		if(cols != null) {
			column = checkCol(column);
			
			log.debug("getColumnLabel column : " + column + "---" + colsMeta[column]);
			return cols[column];	
		}
		
		return null;
	}
	
    public int checkCol(int col) throws SQLException {
        if (colsMeta == null) {
            throw new IllegalStateException("SQLite JDBC: inconsistent internal state");
        }
        if (col < 1 || col > colsMeta.length) {
            throw new SQLException("column " + col + " out of bounds [1," + colsMeta.length + "]");
        }
        return --col;
    }

	@Override
	public String getColumnName(int column) throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		if(cols != null) {
			column = checkCol(column);
			
			log.debug("getColumnName column : " + column + "---" + cols[column]);
			return cols[column];	
		}
		
		return null;
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		
		// TODO: need further test
		return 1000;
	}

	@Override
	public int getScale(int column) throws SQLException {
		
		if (getColumnType(column) != Types.FLOAT) {
			return 0;
		} else {
			throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		}
	}

	@Override
	public String getTableName(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		/* only being used in native query and result is not entity */
		if(cols != null) {
			String typeName = getColumnTypeName(column);
			log.debug("typeName: " + typeName);
			if ("BOOLEAN".equals(typeName)) {
                return Types.BOOLEAN;
            }

            if ("TINYINT".equals(typeName)) {
                return Types.TINYINT;
            }

            if ("SMALLINT".equals(typeName) || "INT2".equals(typeName)) {
                return Types.SMALLINT;
            }

            if ("BIGINT".equals(typeName)
                    || "INT8".equals(typeName)
                    || "UNSIGNED BIG INT".equals(typeName)) {
                return Types.BIGINT;
            }

            if ("DATE".equals(typeName) || "DATETIME".equals(typeName)) {
                return Types.DATE;
            }

            if ("TIMESTAMP".equals(typeName)) {
                return Types.TIMESTAMP;
            }

            if ("INT".equals(typeName) || "INTEGER".equals(typeName) || "MEDIUMINT".equals(typeName)) {
                return Types.INTEGER;
            }
            
            if ("DECIMAL".equals(typeName)) {
                return Types.DECIMAL;
            }

            if ("DOUBLE".equals(typeName) || "DOUBLE PRECISION".equals(typeName)) {
                return Types.DOUBLE;
            }

            if ("NUMERIC".equals(typeName)) {
                return Types.NUMERIC;
            }

            if ("REAL".equals(typeName)) {
                return Types.REAL;
            }

            if ("FLOAT".equals(typeName)) {
                return Types.FLOAT;
            }
            
            if ("CHARACTER".equals(typeName)
                    || "NCHAR".equals(typeName)
                    || "NATIVE CHARACTER".equals(typeName)
                    || "CHAR".equals(typeName)) {
                return Types.CHAR;
            }

            if ("CLOB".equals(typeName)) {
                return Types.CLOB;
            }

            if ("VARCHAR".equals(typeName)
                    || "VARYING CHARACTER".equals(typeName)
                    || "NVARCHAR".equals(typeName)
                    || "TEXT".equals(typeName)) {
                return Types.VARCHAR;
            }
            
            if ("BINARY".equals(typeName)) {
                return Types.BINARY;
            }

            if ("BLOB".equals(typeName)) {
                return Types.BLOB;
            }
            
            return Types.VARCHAR; 
		}
		
		return Types.NULL;
	}

	/* get from rqlite response body.results.types */
	@Override
	public String getColumnTypeName(int column) throws SQLException {
		
		column = checkCol(column);
		
		return colsMeta[column].toUpperCase();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		log.debug(new Object(){}.getClass().getEnclosingMethod().getName() + " column : " + column);
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		log.debug(new Object(){}.getClass().getEnclosingMethod().getName() + " column : " + column);
		return true;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean next() throws SQLException {

		if(values != null) {
			
			if (!open) {
				return false; // finished ResultSet
			}
			lastCol = -1;
	
			// first row is loaded by execute(), so do not step() again
			if (row == -1) {
				row++;
				return true;
			}
	
			// check if we are row limited by the statement or the ResultSet
			if (maxRows != 0 && row == maxRows-1) {
				return false;
			}
	
			// do the real work
			row++;
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public void close() throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		// TODO
	}

	@Override
	public boolean wasNull() throws SQLException {
		if(maxRows > 0) {
			return false;
		}
		else {
			return true;
		}
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result, false)) {
	
				return result.toString();	
			}
		}
		return null;
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				return Boolean.parseBoolean(result.toString());
			}
		}
		return false;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				return Short.parseShort(result.toString());
			}
		}
		return 0;
	}

	private boolean isNotNull(Object result) {
		return isNotNull(result, true);
	}
	
	private boolean isNotNull(Object result, boolean checkBlank) {
		if(result != null ) {

			log.debug("isNotNull result " + gson.toJson(result));
			
			if(gson.toJson(result).equals("{}") ) {
				return false;
			}
			
			if(checkBlank && StringUtils.isBlank(result.toString())) {
				return false;
			}
			
			return true;
		}
		return false;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
				return Integer.parseInt(result.toString());
			}
		}
		return 0;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
				return Long.parseLong(result.toString());
			}
		}
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				return Float.parseFloat(result.toString());
			}
		}
		return 0;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				return Double.parseDouble(result.toString());
			}
		}
		return 0;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				return new BigDecimal(result.toString()).setScale(scale);
			}
		}
		return null;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if (isNotNull(result)) {
				if (result instanceof byte[]) {
					return (byte[]) result;
				} else if (result instanceof String) {
					return Base64.getDecoder().decode(((String) result).getBytes());
				} else {
					throw new SQLException("not supported type '" + result.getClass().getName() + "'");
				}
			}
		}
		return null;
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {

		        if (result instanceof String) {
		        	try {
		        		return new Date(sdf.parse(result.toString()).getTime());
					} catch (ParseException e) {
						log.debug("cols " + result.toString());
					}
		        }

                return new Date(new Long(result.toString()) * conn.getDatabase().getConfig().dateMultiplier);
			}
		}
		return null;
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {

		        if (result instanceof String) {
		        	try {
		        		return new Time(sdf.parse(result.toString()).getTime());
					} catch (ParseException e) {
						log.debug("cols " + result.toString());
					}
		        }

                return new Time(new Long(result.toString()) * conn.getDatabase().getConfig().dateMultiplier);
			}
		}
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
				log.debug("cols " + result.toString());

		        if (result instanceof String) {
		        	try {
		        		return new Timestamp(sdf.parse(result.toString()).getTime());
					} catch (ParseException e) {
						log.debug("cols " + result.toString());
					}
		        }

		        return new Timestamp(new Long(result.toString()) * conn.getDatabase().getConfig().dateMultiplier);
			}
		}
		return null;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
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
	public String getCursorName() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		return this;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		if(values != null) {			
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				return result;
			}
		}
		return null;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
	}

    protected Integer findColumnIndexInCache(String col) {
        if (columnNameToIndex == null) {
            return null;
        }
        return columnNameToIndex.get(col);
    }

	@Override
	public int findColumn(String columnLabel) throws SQLException {
        Integer index = findColumnIndexInCache(columnLabel);
        if (index != null) {
        	log.debug("findColumn columnLabel : " + columnLabel + " index : " + index);
            return index;
        }
        for (int i=0; i < cols.length; i++) {
            if (columnLabel.equalsIgnoreCase(cols[i])) {
                return addColumnIndexInCache(columnLabel, i);
            }
        }
        throw new SQLException("no such column: '"+columnLabel+"'");
	}

    protected int addColumnIndexInCache(String col, int index) {
        if (columnNameToIndex == null) {
            columnNameToIndex = new HashMap<String, Integer>(cols.length);
        }
        index++; // Change index start from 1
        
    	log.debug("addColumnIndexInCache findColumn columnLabel : " + col + " index : " + index);
        columnNameToIndex.put(col, index);
        return index;
    }

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(columnLabel, getScale(findColumn(columnLabel)));
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void afterLast() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public boolean first() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean last() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public int getRow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean previous() throws SQLException {
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
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getType() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public Statement getStatement() throws SQLException {
		return stmt;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if (isNotNull(result)) {
				if (result instanceof byte[]) {
					return new SerialBlob((byte[]) result);
				} else if (result instanceof String) {
					return new SerialBlob(Base64.getDecoder().decode(((String) result).getBytes()));
				} else {
					throw new SQLException("not supported type '" + result.getClass().getName() + "'");
				}
			}
		}
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {

		        if (result instanceof String) {
		        	try {
		                cal.setTimeInMillis(sdf.parse(result.toString()).getTime());
		                return new Date(cal.getTime().getTime());
					} catch (ParseException e) {
						log.debug("cols " + result.toString());
					}
		        }

                cal.setTimeInMillis(new Long(result.toString()) * conn.getDatabase().getConfig().dateMultiplier);
                return new Date(cal.getTime().getTime());
			}
		}
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {

		        if (result instanceof String) {
		        	try {
		                cal.setTimeInMillis(sdf.parse(result.toString()).getTime());
		                return new Time(cal.getTime().getTime());
					} catch (ParseException e) {
						log.debug("cols " + result.toString());
					}
		        }

                cal.setTimeInMillis(new Long(result.toString()) * conn.getDatabase().getConfig().dateMultiplier);
                return new Time(cal.getTime().getTime());
			}
		}
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {

		        if (result instanceof String) {
		        	try {
		                cal.setTimeInMillis(sdf.parse(result.toString()).getTime());
		                return new Timestamp(cal.getTime().getTime());
					} catch (ParseException e) {
						log.debug("cols " + result.toString());
					}
		        }
		        
				cal.setTimeInMillis(new Long(result.toString()) * conn.getDatabase().getConfig().dateMultiplier);
                return new Timestamp(cal.getTime().getTime());
			}
		}
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				try {
					return new URL(result.toString());
				} catch (MalformedURLException e) {
					throw new SQLException(e.getMessage());
				}
			}
		}
		return null;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		if(values != null) {
			Object result = values[row][checkCol(columnIndex)];
			if(isNotNull(result)) {
	
				return gson.fromJson(result.toString(), type);
			}
		}
		return null;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
	}

}
