package com.webcomm.rqlite.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.webcomm.rqlite.RQLiteConnection;
import com.webcomm.rqlite.core.CoreDatabaseMetaData.ImportedKeyFinder.ForeignKey;
import com.webcomm.rqlite.util.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoreDatabaseMetaData implements DatabaseMetaData {

	Gson gson = new Gson();
    protected RQLiteConnection conn;
    
    public CoreDatabaseMetaData(RQLiteConnection conn) {
        this.conn = conn;
		log.debug("CoreDatabaseMetaData");
    }
    
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
////		return false;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public String getURL() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getUserName() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		log.debug(new Object(){}.getClass().getEnclosingMethod().getName());
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		return "RQlite";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getDriverName() throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		return "RQlite";
	}

	@Override
	public String getDriverVersion() throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
		return "1.0.0-Ted";
	}

	@Override
	public int getDriverMajorVersion() { //TODO
		return 1;
	}

	@Override
	public int getDriverMinorVersion() { //TODO
		return 0;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getStringFunctions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException {
		PreparedStatement getProcedures = conn.prepareStatement("select null as PROCEDURE_CAT, null as PROCEDURE_SCHEM, " +
                    "null as PROCEDURE_NAME, null as UNDEF1, null as UNDEF2, null as UNDEF3, " +
                    "null as REMARKS, null as PROCEDURE_TYPE limit 0;");
        return getProcedures.executeQuery();
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

    /**
     * Applies SQL escapes for special characters in a given string.
     * @param val The string to escape.
     * @return The SQL escaped string.
     */
    protected String escape(final String val) {
        // TODO: this function is ugly, pass this work off to SQLite, then we
        //       don't have to worry about Unicode 4, other characters needing
        //       escaping, etc.
        int len = val.length();
        StringBuilder buf = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            if (val.charAt(i) == '\'') {
                buf.append('\'');
            }
            buf.append(val.charAt(i));
        }
        return buf.toString();
    }

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		log.debug("catalog " + gson.toJson(catalog));
		log.debug("schemaPattern " + gson.toJson(schemaPattern));
		log.debug("tableNamePattern " + gson.toJson(tableNamePattern));
		log.debug("types " + gson.toJson(types));

		try {

			tableNamePattern = (tableNamePattern == null || "".equals(tableNamePattern)) ? "%" : escape(tableNamePattern);

	        StringBuilder sql = new StringBuilder();
	        sql.append("SELECT").append("\n");
	        sql.append("  NULL AS TABLE_CAT,").append("\n");
	        sql.append("  NULL AS TABLE_SCHEM,").append("\n");
	        sql.append("  NAME AS TABLE_NAME,").append("\n");
	        sql.append("  TYPE AS TABLE_TYPE,").append("\n");
	        sql.append("  NULL AS REMARKS,").append("\n");
	        sql.append("  NULL AS TYPE_CAT,").append("\n");
	        sql.append("  NULL AS TYPE_SCHEM,").append("\n");
	        sql.append("  NULL AS TYPE_NAME,").append("\n");
	        sql.append("  NULL AS SELF_REFERENCING_COL_NAME,").append("\n");
	        sql.append("  NULL AS REF_GENERATION").append("\n");
	        sql.append("FROM").append("\n");
	        sql.append("  (").append("\n");
	        sql.append("    SELECT").append("\n");
	        sql.append("      NAME,").append("\n");
	        sql.append("      UPPER(TYPE) AS TYPE").append("\n");
	        sql.append("    FROM").append("\n");
	        sql.append("      sqlite_master").append("\n");
	        sql.append("    WHERE").append("\n");
	        sql.append("      NAME NOT LIKE 'sqlite_%'").append("\n");
	        sql.append("      AND UPPER(TYPE) IN ('TABLE', 'VIEW')").append("\n");
	        sql.append("    UNION ALL").append("\n");
	        sql.append("    SELECT").append("\n");
	        sql.append("      NAME,").append("\n");
	        sql.append("      'GLOBAL TEMPORARY' AS TYPE").append("\n");
	        sql.append("    FROM").append("\n");
	        sql.append("      sqlite_temp_master").append("\n");
	        sql.append("    UNION ALL").append("\n");
	        sql.append("    SELECT").append("\n");
	        sql.append("      NAME,").append("\n");
	        sql.append("      'SYSTEM TABLE' AS TYPE").append("\n");
	        sql.append("    FROM").append("\n");
	        sql.append("      sqlite_master").append("\n");
	        sql.append("    WHERE").append("\n");
	        sql.append("      NAME LIKE 'sqlite_%'").append("\n");
	        sql.append("  )").append("\n");
	        sql.append(" WHERE TABLE_NAME LIKE '").append(tableNamePattern).append("' AND TABLE_TYPE IN (");

	        if (types == null || types.length == 0) {
	            sql.append("'TABLE','VIEW'");
	        }
	        else {
	            sql.append("'").append(types[0].toUpperCase()).append("'");

	            for (int i = 1; i < types.length; i++) {
	                sql.append(",'").append(types[i].toUpperCase()).append("'");
	            }
	        }

	        sql.append(") ORDER BY TABLE_TYPE, TABLE_NAME;");
			
	        return ((CoreStatement)conn.createStatement()).executeQuery(sql.toString());
		} catch (SQLException e) {
			throw e;
		}
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());

        String sql = "SELECT 'TABLE' AS TABLE_TYPE " +
        		"UNION " +
        		"SELECT 'VIEW' AS TABLE_TYPE " +
        		"UNION " +
        		"SELECT 'SYSTEM TABLE' AS TABLE_TYPE " +
        		"UNION " +
        		"SELECT 'GLOBAL TEMPORARY' AS TABLE_TYPE;";
        
		PreparedStatement getProcedures = conn.prepareStatement(sql);
        return getProcedures.executeQuery();
	}

    /**
     * Adds SQL string quotes to the given string.
     * @param tableName The string to quote.
     * @return The quoted string.
     */
    protected static String quote(String tableName) {
        if (tableName == null) {
            return "null";
        }
        else {
            return String.format("'%s'", tableName);
        }
    }
	
    // Column type patterns
    protected static final Pattern TYPE_INTEGER = Pattern.compile(".*(INT|BOOL).*");
    protected static final Pattern TYPE_VARCHAR = Pattern.compile(".*(CHAR|CLOB|TEXT|BLOB).*");
    protected static final Pattern TYPE_FLOAT = Pattern.compile(".*(REAL|FLOA|DOUB|DEC|NUM).*");

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {

		log.debug("catalog " + gson.toJson(catalog));
		log.debug("schemaPattern " + gson.toJson(schemaPattern));
		log.debug("tableNamePattern " + gson.toJson(tableNamePattern));
		log.debug("columnNamePattern " + gson.toJson(columnNamePattern));
		
        StringBuilder sql = new StringBuilder(700);
        sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, tblname as TABLE_NAME, ")
           .append("cn as COLUMN_NAME, ct as DATA_TYPE, tn as TYPE_NAME, 2000000000 as COLUMN_SIZE, ")
           .append("2000000000 as BUFFER_LENGTH, 10   as DECIMAL_DIGITS, 10   as NUM_PREC_RADIX, ")
           .append("colnullable as NULLABLE, null as REMARKS, colDefault as COLUMN_DEF, ")
           .append("0    as SQL_DATA_TYPE, 0    as SQL_DATETIME_SUB, 2000000000 as CHAR_OCTET_LENGTH, ")
           .append("ordpos as ORDINAL_POSITION, (case colnullable when 0 then 'NO' when 1 then 'YES' else '' end)")
           .append("    as IS_NULLABLE, null as SCOPE_CATLOG, null as SCOPE_SCHEMA, ")
           .append("null as SCOPE_TABLE, null as SOURCE_DATA_TYPE, ")
           .append("(case colautoincrement when 0 then 'NO' when 1 then 'YES' else '' end) as IS_AUTOINCREMENT, ")
           .append("'' as IS_GENERATEDCOLUMN from (");

        boolean colFound = false;
                      
        ResultSet rs = null;
        try {
            // Get all tables implied by the input
            final String[] types = new String[] {"TABLE", "VIEW"};
            rs = getTables(catalog, schemaPattern, tableNamePattern, types);
            while (rs.next()) {
                String tableName = rs.getString(3);

                boolean isAutoIncrement = false;  
                
                Statement statColAutoinc = conn.createStatement();
                ResultSet rsColAutoinc = null;
                try {
                	statColAutoinc = conn.createStatement();
                	rsColAutoinc = statColAutoinc.executeQuery("SELECT LIKE('%autoincrement%', LOWER(sql)) FROM sqlite_master "
                			+ "WHERE LOWER(name) = LOWER('" + escape(tableName) + "') AND TYPE IN ('table', 'view')");
                	rsColAutoinc.next();
                	isAutoIncrement = rsColAutoinc.getInt(1) == 1;
                }  finally {
                	if (rsColAutoinc != null) {
                			try {
                					rsColAutoinc.close();
                			} catch (Exception e) {
                					e.printStackTrace();
                			}
                	}
                	if (statColAutoinc != null) {
                			try {
                					statColAutoinc.close();
                			} catch (Exception e) {
                					e.printStackTrace();
                			}
                	}	
                }
                
                Statement colstat = conn.createStatement();
                ResultSet rscol = null;
                try {
                    // For each table, get the column info and build into overall SQL
                    String pragmaStatement = "PRAGMA table_info('"+ tableName + "')";
                    rscol = colstat.executeQuery(pragmaStatement);

                    for (int i = 0; rscol.next(); i++) {
                        String colName = rscol.getString(2);
                        String colType = rscol.getString(3);
                        String colNotNull = rscol.getString(4);
                        String colDefault = rscol.getString(5);
                        boolean isPk = "1".equals(rscol.getString(6));

                        int colNullable = 2;
                        if (colNotNull != null) {
                            colNullable = colNotNull.equals("0") ? 1 : 0;
                        }

                        if (colFound) {
                            sql.append(" union all ");
                        }
                        colFound = true;

                        /*
                         * improved column types
                         * ref http://www.sqlite.org/datatype3.html - 2.1 Determination Of Column Affinity
                         * plus some degree of artistic-license applied
                         */
                        colType = colType == null ? "TEXT" : colType.toUpperCase();

                        int colAutoIncrement = 0;
                        if(isPk && isAutoIncrement)
                        {
                            colAutoIncrement = 1;
                        }
                        int colJavaType = -1;
                        // rule #1 + boolean
                        if (TYPE_INTEGER.matcher(colType).find()) {
                            colJavaType = Types.INTEGER;
                        }
                        else if (TYPE_VARCHAR.matcher(colType).find()) {
                            colJavaType = Types.VARCHAR;
                        }
                        else if (TYPE_FLOAT.matcher(colType).find()) {
                            colJavaType = Types.FLOAT;
                        }
                        else {
                            // catch-all
                            colJavaType = Types.VARCHAR;
                        }

                        sql.append("select ").append(i + 1).append(" as ordpos, ")
                           .append(colNullable).append(" as colnullable,")
                           .append("'").append(colJavaType).append("' as ct, ")
                           .append("'").append(tableName).append("' as tblname, ")
                           .append("'").append(escape(colName)).append("' as cn, ")
                           .append("'").append(escape(colType)).append("' as tn, ")
                           .append(quote(colDefault == null ? null : escape(colDefault))).append(" as colDefault,")
                           .append(colAutoIncrement).append(" as colautoincrement");

                        if (columnNamePattern != null) {
                            sql.append(" where upper(cn) like upper('").append(escape(columnNamePattern)).append("')");
                        }
                    }
                } finally {
                    if (rscol != null) {
                        try {
                            rscol.close();
                        } catch (SQLException e) {}
                    }
                    if (colstat != null) {
                        try {
                            colstat.close();
                        } catch(SQLException e) {}
                    }
                }
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if (colFound) {
            sql.append(") order by TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;");
        }
        else {
            sql.append("select null as ordpos, null as colnullable, null as ct, null as tblname, null as cn, null as tn, null as colDefault, null as colautoincrement) limit 0;");
        }

        Statement stat = conn.createStatement();
        return ((CoreStatement)stat).executeQuery(sql.toString());
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
    public ResultSet getPrimaryKeys(String c, String s, String table) throws SQLException {
        PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(table);
        String[] columns = pkFinder.getColumns();

        Statement stat = conn.createStatement();
        StringBuilder sql = new StringBuilder(512);
        sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
           .append(escape(table))
           .append("' as TABLE_NAME, cn as COLUMN_NAME, ks as KEY_SEQ, pk as PK_NAME from (");

        if (columns == null) {
            sql.append("select null as cn, null as pk, 0 as ks) limit 0;");

            return ((CoreStatement)stat).executeQuery(sql.toString());
        }

        String pkName = pkFinder.getName();
        if (pkName != null) {
        	pkName = "'" + pkName + "'";
        }

        for (int i = 0; i < columns.length; i++) {
            if (i > 0) sql.append(" union ");
            sql.append("select ").append(pkName).append(" as pk, '")
               .append(escape(unquoteIdentifier(columns[i]))).append("' as cn, ")
               .append(i+1).append(" as ks");
        }

        return ((CoreStatement)stat).executeQuery(sql.append(") order by cn;").toString());
    }

    private StringBuilder appendDummyForeignKeyList(StringBuilder sql) {
      sql.append("select -1 as ks, '' as ptn, '' as fcn, '' as pcn, ")
      .append(DatabaseMetaData.importedKeyNoAction).append(" as ur, ")
      .append(DatabaseMetaData.importedKeyNoAction).append(" as dr, ")
      .append(" '' as fkn, ")
      .append(" '' as pkn ")
      .append(") limit 0;");
      return sql;
    }
    
    class ImportedKeyFinder {
    	
        /**
         * Pattern used to extract a named primary key.
         */
         private final Pattern FK_NAMED_PATTERN =
            Pattern.compile("CONSTRAINT\\s*([A-Za-z_][A-Za-z\\d_]*)?\\s*FOREIGN\\s+KEY\\s*\\((.*?)\\)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
         
    	private String fkTableName;
    	private List<ForeignKey> fkList = new ArrayList<ForeignKey>();
    	
    	public ImportedKeyFinder(String table) throws SQLException {

            if (table == null || table.trim().length() == 0) {
                throw new SQLException("Invalid table name: '" + table + "'");
            }

            this.fkTableName = table;
            
            List<String> fkNames = getForeignKeyNames(this.fkTableName);

            Statement stat = null;
            ResultSet rs = null;

            try {
                stat = conn.createStatement();
                rs = stat.executeQuery("pragma foreign_key_list('" + this.fkTableName.toLowerCase()
                		+ "')");

                int prevFkId = -1;
                int count = 0;
                ForeignKey fk = null;
                while(rs.next()) {
                	int fkId = rs.getInt(1);
                	int colSeq = rs.getInt(2);
                	String pkTableName = rs.getString(3);
                	String fkColName = rs.getString(4);
                	String pkColName = rs.getString(5);
                	String onUpdate = rs.getString(6);
                	String onDelete = rs.getString(7);
                	String match = rs.getString(8);
                	
                	String fkName = null;
                    if (fkNames.size() > count) fkName = fkNames.get(count);
                    
                	if (fkId != prevFkId) {
                		fk = new ForeignKey(fkName, pkTableName, fkTableName, onUpdate, onDelete, match);
                		fkList.add(fk);
                		prevFkId = fkId;
                		count++;
                	}
                	fk.addColumnMapping(fkColName, pkColName);
                }
            }
            finally {
                try {
                    if (rs != null) rs.close();
                } catch (Exception e) {}
                try {
                    if (stat != null) stat.close();
                } catch (Exception e) {}
            }
        }
    	
    	private List<String> getForeignKeyNames(String tbl) throws SQLException {		
    		List<String> fkNames = new ArrayList<String>();
    		if (tbl==null) {
    			return fkNames;
    		}
    		Statement stat2 = null;
    		ResultSet rs = null;
    		try {
    			stat2 = conn.createStatement();

    			rs = stat2.executeQuery(
    					"select sql from sqlite_master where" + " lower(name) = lower('" + escape(tbl) + "')");
    			if (rs.next()) {
    				Matcher matcher = FK_NAMED_PATTERN.matcher(rs.getString(1));

    				while (matcher.find()) {
    					fkNames.add(matcher.group(1));
    				}
    			}
    		} finally {
    			try {
    				if (rs != null)
    					rs.close();
    			} catch (SQLException e) {
    			}
    			try {
    				if (stat2 != null)
    					stat2.close();
    			} catch (SQLException e) {
    			}
    		}
    		Collections.reverse(fkNames);
    		return fkNames;
    	}
    	
    	public String getFkTableName() {
			return fkTableName;
		}

		public List<ForeignKey> getFkList() {
			return fkList;
		}

		class ForeignKey {
			
			private String fkName;
			private String pkTableName;
    		private String fkTableName;
    		private List<String> fkColNames = new ArrayList<String>();
    		private List<String> pkColNames = new ArrayList<String>();
    		private String onUpdate;
    		private String onDelete;
    		private String match;
    		
    		ForeignKey(String fkName, String pkTableName, String fkTableName, String onUpdate, String onDelete, String match) {
				this.fkName = fkName;
				this.pkTableName = pkTableName;
				this.fkTableName = fkTableName;
				this.onUpdate = onUpdate;
				this.onDelete = onDelete;
				this.match = match;
			}
    		

    		public String getFkName() {
				return fkName;
			}

			void addColumnMapping(String fkColName, String pkColName) {
    			fkColNames.add(fkColName);
    			pkColNames.add(pkColName);
    		}
    		
    		public String[] getColumnMapping(int colSeq) {
    			return new String[] {fkColNames.get(colSeq), pkColNames.get(colSeq)};
    		}
    		
    		public int getColumnMappingCount() {
    			return fkColNames.size();
    		}

			public String getPkTableName() {
				return pkTableName;
			}

			public String getFkTableName() {
				return fkTableName;
			}

			public String getOnUpdate() {
				return onUpdate;
			}

			public String getOnDelete() {
				return onDelete;
			}

			public String getMatch() {
				return match;
			}


			@Override
			public String toString() {
				return "ForeignKey [fkName=" + fkName + ", pkTableName=" + pkTableName + ", fkTableName=" + fkTableName
						+ ", pkColNames=" + pkColNames + ", fkColNames=" + fkColNames + "]";
			}
    	}
    	
    }
    
    /**
     * Follow rules in <a href="https://sqlite.org/lang_keywords.html">SQLite Keywords</a>
     * @param name Identifier name
     * @return Unquoted identifier
     */
    private String unquoteIdentifier(String name) {	
    	if (name == null) return name;
    	name = name.trim();
        if (name.length() > 2 && (
        		(name.startsWith("`") && name.endsWith("`"))
        	||	(name.startsWith("\"") && name.endsWith("\""))
        	||	(name.startsWith("[") && name.endsWith("]"))
        	)) {
        	// unquote to be consistent with column names returned by getColumns()
        	name = name.substring(1, name.length() - 1);
        }
		return name;
    }

    /**
     * Pattern used to extract column order for an unnamed primary key.
     */
    protected final static Pattern PK_UNNAMED_PATTERN =
        Pattern.compile(".*PRIMARY\\s+KEY\\s*\\((.*?)\\).*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    
    /**
     * Pattern used to extract a named primary key.
     */
     protected final static Pattern PK_NAMED_PATTERN =
         Pattern.compile(".*CONSTRAINT\\s*(.*?)\\s*PRIMARY\\s+KEY\\s*\\((.*?)\\).*",
             Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
     
    /**
     * Parses the sqlite_master table for a table's primary key
     */
    class PrimaryKeyFinder {
        /** The table name. */
        String table;

        /** The primary key name. */
        String pkName = null;

        /** The column(s) for the primary key. */
        String pkColumns[] = null;

        /**
         * Constructor.
         * @param table The table for which to get find a primary key.
         * @throws SQLException
         */
        public PrimaryKeyFinder(String table) throws SQLException {
            this.table = table;

            if (table == null || table.trim().length() == 0) {
                throw new SQLException("Invalid table name: '" + this.table + "'");
            }

            Statement stat = null;
            ResultSet rs = null;

            try {
                stat = conn.createStatement();
                // read create SQL script for table
                rs = stat.executeQuery("select sql from sqlite_master where" +
                    " lower(name) = lower('" + escape(table) + "') and type in ('table', 'view')");

                if (!rs.next())
                    throw new SQLException("Table not found: '" + table + "'");

                Matcher matcher = PK_NAMED_PATTERN.matcher(rs.getString(1));
                if (matcher.find()){
                    pkName = unquoteIdentifier(escape(matcher.group(1)));
                    pkColumns = matcher.group(2).split(",");
                }
                else {
                    matcher = PK_UNNAMED_PATTERN.matcher(rs.getString(1));
                    if (matcher.find()){
                        pkColumns = matcher.group(1).split(",");
                    }
                }

                if (pkColumns == null) {
                    rs = stat.executeQuery("pragma table_info('" + escape(table) + "');");
                    while(rs.next()) {
                        if (rs.getBoolean(6))
                            pkColumns = new String[]{rs.getString(2)};
                    }
                }

                if (pkColumns != null) {
                    for (int i = 0; i < pkColumns.length; i++) {
                        pkColumns[i] = unquoteIdentifier(pkColumns[i]);
                    }
                }
            }
            finally {
                try {
                    if (rs != null) rs.close();
                } catch (Exception e) {}
                try {
                    if (stat != null) stat.close();
                } catch (Exception e) {}
            }
        }

        /**
         * @return The primary key name if any.
         */
        public String getName() {
            return pkName;
        }

        /**
         * @return Array of primary key column(s) if any.
         */
        public String[] getColumns() {
            return pkColumns;
        }
    }

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet rs = null;
        Statement stat = conn.createStatement();
        StringBuilder sql = new StringBuilder(700);

        sql.append("select ").append(quote(catalog)).append(" as PKTABLE_CAT, ")
            .append(quote(schema)).append(" as PKTABLE_SCHEM, ")
            .append("ptn as PKTABLE_NAME, pcn as PKCOLUMN_NAME, ")
            .append(quote(catalog)).append(" as FKTABLE_CAT, ")
            .append(quote(schema)).append(" as FKTABLE_SCHEM, ")
            .append(quote(table)).append(" as FKTABLE_NAME, ")
            .append("fcn as FKCOLUMN_NAME, ks as KEY_SEQ, ur as UPDATE_RULE, dr as DELETE_RULE, fkn as FK_NAME, pkn as PK_NAME, ")
            .append(Integer.toString(DatabaseMetaData.importedKeyInitiallyDeferred)).append(" as DEFERRABILITY from (");

        // Use a try catch block to avoid "query does not return ResultSet" error
        try {
            rs = stat.executeQuery("pragma foreign_key_list('" + escape(table) + "');");
        }
        catch (SQLException e) {
            sql = appendDummyForeignKeyList(sql);
            return ((CoreStatement)stat).executeQuery(sql.toString());
        }
        
    	final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(table);
    	List<ImportedKeyFinder.ForeignKey> fkNames = impFkFinder.getFkList();  

        int i = 0;
        for (; rs.next(); i++) {
            int keySeq = rs.getInt(2) + 1;
            int keyId = rs.getInt(1);
            String PKTabName = rs.getString(3);
            String FKColName = rs.getString(4);
            String PKColName = rs.getString(5);

            PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(PKTabName);
            String pkName = pkFinder.getName();
            if (PKColName == null) {
				PKColName = pkFinder.getColumns()[0];
            }

            String updateRule = rs.getString(6);
            String deleteRule = rs.getString(7);

            if (i > 0) {
                sql.append(" union all ");
            }

            String fkName = null;
            if (fkNames.size() > keyId) fkName = fkNames.get(keyId).getFkName();
            
            sql.append("select ").append(keySeq).append(" as ks,")
                .append("'").append(escape(PKTabName)).append("' as ptn, '")
                .append(escape(FKColName)).append("' as fcn, '")
                .append(escape(PKColName)).append("' as pcn,")
                .append("case '").append(escape(updateRule)).append("'")
                .append(" when 'NO ACTION' then ").append(DatabaseMetaData.importedKeyNoAction)
                .append(" when 'CASCADE' then ").append(DatabaseMetaData.importedKeyCascade)
                .append(" when 'RESTRICT' then ").append(DatabaseMetaData.importedKeyRestrict)
                .append(" when 'SET NULL' then ").append(DatabaseMetaData.importedKeySetNull)
                .append(" when 'SET DEFAULT' then ").append(DatabaseMetaData.importedKeySetDefault).append(" end as ur, ")
                .append("case '").append(escape(deleteRule)).append("'")
                .append(" when 'NO ACTION' then ").append(DatabaseMetaData.importedKeyNoAction)
                .append(" when 'CASCADE' then ").append(DatabaseMetaData.importedKeyCascade)
                .append(" when 'RESTRICT' then ").append(DatabaseMetaData.importedKeyRestrict)
                .append(" when 'SET NULL' then ").append(DatabaseMetaData.importedKeySetNull)
                .append(" when 'SET DEFAULT' then ").append(DatabaseMetaData.importedKeySetDefault).append(" end as dr, ")
                .append(fkName == null? "''": quote(fkName)).append(" as fkn, ")
                .append(pkName == null? "''": quote(pkName)).append(" as pkn");
        }
        rs.close();

        if(i == 0) {
          sql = appendDummyForeignKeyList(sql);
        }
        sql.append(") ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ;");
        
        return ((CoreStatement)stat).executeQuery(sql.toString());
	}

    private final static Map<String, Integer> RULE_MAP = new HashMap<String, Integer>();

    static {
        RULE_MAP.put("NO ACTION", DatabaseMetaData.importedKeyNoAction);
        RULE_MAP.put("CASCADE", DatabaseMetaData.importedKeyCascade);
        RULE_MAP.put("RESTRICT", DatabaseMetaData.importedKeyRestrict);
        RULE_MAP.put("SET NULL", DatabaseMetaData.importedKeySetNull);
        RULE_MAP.put("SET DEFAULT", DatabaseMetaData.importedKeySetDefault);
    }

	@Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(table);
        String[] pkColumns = pkFinder.getColumns();
        Statement stat = conn.createStatement();

        catalog = (catalog != null) ? quote(catalog) : null;
        schema = (schema != null) ? quote(schema) : null;

        StringBuilder exportedKeysQuery = new StringBuilder(512);

        String target = null;
        int count = 0;
        if (pkColumns != null) {
            // retrieve table list
            ResultSet rs = stat.executeQuery("select name from sqlite_master where type = 'table'");
            ArrayList<String> tableList = new ArrayList<String>();

            while (rs.next()) {
            	String tblname = rs.getString(1);
                tableList.add(tblname);
                if (tblname.equalsIgnoreCase(table)) {
                	// get the correct case as in the database
                	// (not uppercase nor lowercase)
                	target = tblname;
                }
            }

            rs.close();

            // find imported keys for each table
            for (String tbl : tableList) {
                try {
                	final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(tbl);
                	List<ForeignKey> fkNames = impFkFinder.getFkList();  
                	
                	for (Iterator iterator = fkNames.iterator(); iterator.hasNext();) {
						ForeignKey foreignKey = (ForeignKey) iterator.next();
						
                        String PKTabName = foreignKey.getPkTableName();

                        if (PKTabName == null || !PKTabName.equalsIgnoreCase(target)) {
                            continue;
                        }
                        
                        for (int j = 0; j < foreignKey.getColumnMappingCount(); j++) {
	                        int keySeq = j + 1;
	                        String[] columnMapping = foreignKey.getColumnMapping(j);
	                        String PKColName = columnMapping[1];
	                        PKColName = (PKColName == null) ? "" : PKColName;
	                        String FKColName = columnMapping[0];
	                        FKColName = (FKColName == null) ? "" : FKColName;
	                        
	                        boolean usePkName = false;
	                        for (int k = 0; k < pkColumns.length; k++) {
								if (pkColumns[k] != null && pkColumns[k].equalsIgnoreCase(PKColName)) {
									usePkName = true;
									break;
								}
							}
	                        String pkName = (usePkName && pkFinder.getName() != null)? pkFinder.getName(): "";
	                        	
	                        exportedKeysQuery
	                            .append(count > 0 ? " union all select " : "select ")
	                            .append(Integer.toString(keySeq)).append(" as ks, '")
	                            .append(escape(tbl)).append("' as fkt, '")
	                            .append(escape(FKColName)).append("' as fcn, '")
	                            .append(escape(PKColName)).append("' as pcn, '")
	                            .append(escape(pkName)).append("' as pkn, ")
	                            .append(RULE_MAP.get(foreignKey.getOnUpdate())).append(" as ur, ")
	                            .append(RULE_MAP.get(foreignKey.getOnDelete())).append(" as dr, ");
	
	                        String fkName = foreignKey.getFkName();
	                        
	                        if (fkName != null){
	                            exportedKeysQuery.append("'").append(escape(fkName)).append("' as fkn");
	                        }
	                        else {
	                            exportedKeysQuery.append("'' as fkn");
	                        }
	                        
	                        count++;
	                    }
                	}
                }
                finally {
                    try{
                        if (rs != null) rs.close();
                    }catch(SQLException e) {}
                }
            }
        }

        boolean hasImportedKey = (count > 0);
        StringBuilder sql = new StringBuilder(512);
		sql.append("select ")
            .append(catalog).append(" as PKTABLE_CAT, ")
            .append(schema).append(" as PKTABLE_SCHEM, ")
            .append(quote(target)).append(" as PKTABLE_NAME, ")
            .append(hasImportedKey ? "pcn" : "''").append(" as PKCOLUMN_NAME, ")
            .append(catalog).append(" as FKTABLE_CAT, ")
            .append(schema).append(" as FKTABLE_SCHEM, ")
            .append(hasImportedKey ? "fkt" : "''").append(" as FKTABLE_NAME, ")
            .append(hasImportedKey ? "fcn" : "''").append(" as FKCOLUMN_NAME, ")
            .append(hasImportedKey ? "ks" : "-1").append(" as KEY_SEQ, ")
            .append(hasImportedKey ? "ur" : "3").append(" as UPDATE_RULE, ")
            .append(hasImportedKey ? "dr" : "3").append(" as DELETE_RULE, ")
            .append(hasImportedKey ? "fkn" : "''").append(" as FK_NAME, ")
            .append(hasImportedKey ? "pkn" : "''").append(" as PK_NAME, ")
            .append(Integer.toString(DatabaseMetaData.importedKeyInitiallyDeferred)) // FIXME: Check for pragma foreign_keys = true ?
            .append(" as DEFERRABILITY ");

        if (hasImportedKey) {
            sql.append("from (").append(exportedKeysQuery).append(") ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");
        }
        else {
            sql.append("limit 0");
        }

        return ((CoreStatement)stat).executeQuery(sql.toString());
    }

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
        ResultSet rs = null;
        Statement stat = conn.createStatement();
        StringBuilder sql = new StringBuilder(500);

        // define the column header
        // this is from the JDBC spec, it is part of the driver protocol
        sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
                .append(escape(table)).append("' as TABLE_NAME, un as NON_UNIQUE, null as INDEX_QUALIFIER, n as INDEX_NAME, ")
                .append(Integer.toString(DatabaseMetaData.tableIndexOther)).append(" as TYPE, op as ORDINAL_POSITION, ")
                .append("cn as COLUMN_NAME, null as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION from (");

        // this always returns a result set now, previously threw exception
        rs = stat.executeQuery("pragma index_list('" + escape(table) + "');");

        ArrayList<ArrayList<Object>> indexList = new ArrayList<ArrayList<Object>>();
        while (rs.next()) {
            indexList.add(new ArrayList<Object>());
            indexList.get(indexList.size() - 1).add(rs.getString(2));
            indexList.get(indexList.size() - 1).add(rs.getInt(3));
        }
        rs.close();
        if (indexList.size() == 0) {
            // if pragma index_list() returns no information, use this null block
            sql.append("select null as un, null as n, null as op, null as cn) limit 0;");
            return ((CoreStatement) stat).executeQuery(sql.toString());
        } else {
            // loop over results from pragma call, getting specific info for each index

            int i = 0;
            Iterator<ArrayList<Object>> indexIterator = indexList.iterator();
            ArrayList<Object> currentIndex;

            ArrayList<String> unionAll = new ArrayList<String>();

            while (indexIterator.hasNext()) {
                currentIndex = indexIterator.next();
                String indexName = currentIndex.get(0).toString();
                rs = stat.executeQuery("pragma index_info('" + escape(indexName) + "');");

                while (rs.next()) {

                    StringBuilder sqlRow = new StringBuilder();

                    String colName = rs.getString(3);
                    sqlRow.append("select ").append(Integer.toString(1 - (Integer) currentIndex.get(1))).append(" as un,'")
                            .append(escape(indexName)).append("' as n,")
                            .append(Integer.toString(rs.getInt(1) + 1)).append(" as op,");
                    if (colName == null) { // expression index
                      sqlRow.append("null");
                    }
                    else {
                      sqlRow.append("'").append(escape(colName)).append("'");
                    }
                    sqlRow.append(" as cn");

                    unionAll.add(sqlRow.toString());
                }

                rs.close();
            }

            String sqlBlock = StringUtils.join(unionAll, " union all ");

            return ((CoreStatement) stat).executeQuery(sql.append(sqlBlock).append(");").toString());
        }
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public Connection getConnection() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return 0;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
			throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
////		return null;
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
	}

}
