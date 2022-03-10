package com.webcomm.rqlite.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.webcomm.rqlite.RQLiteConnection;

public class CoreDatabaseMetaData implements DatabaseMetaData {

	Gson gson = new Gson();
    protected RQLiteConnection conn;
    
    public CoreDatabaseMetaData(RQLiteConnection conn) {
        this.conn = conn;
		System.out.println("CoreDatabaseMetaData");
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
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return false;
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
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public String getDriverVersion() throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
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
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName()); // TODO for Tools
//		return null;
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
//		String input = "catalog " + gson.toJson(catalog) + "---"
//				+ "schemaPattern " + gson.toJson(schemaPattern) + "---"
//				+ "tableNamePattern " + gson.toJson(tableNamePattern) + "---"
//				+ "types " + gson.toJson(types) + "---";
		System.out.println("catalog " + gson.toJson(catalog));
		System.out.println("schemaPattern " + gson.toJson(schemaPattern));
		System.out.println("tableNamePattern " + gson.toJson(tableNamePattern));
		System.out.println("types " + gson.toJson(types));
//		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName() + "---" + input); // TODO for Tools : Tables, Views, Indexs 
//		return null;

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
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
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
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
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
		throw new SQLException("not implement : " + new Object(){}.getClass().getEnclosingMethod().getName());
//		return null;
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
