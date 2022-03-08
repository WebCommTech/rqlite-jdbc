package com.webcomm.rqlite;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class RQLiteConfig {

    private final Properties pragmaTable;
    public DateClass dateClass;
    public long dateMultiplier;


    public static enum DateClass implements PragmaValue {
        INTEGER;

        public String getValue() {
            return name();
        }

        public static DateClass getDateClass(String dateClass) {
            return DateClass.valueOf(dateClass.toUpperCase());
        }
    }
    
    private static interface PragmaValue
    {
        public String getValue();
    }
    
    /**
     * Default constructor.
     */
    public RQLiteConfig() {
        this(new Properties());
    }

    /**
     * Creates an SQLite configuration object using values from the given
     * property object.
     * @param prop The properties to apply to the configuration.
     */
    public RQLiteConfig(Properties prop) {
        this.pragmaTable = prop;

        this.dateClass = DateClass.getDateClass(DateClass.INTEGER.name());
        this.dateMultiplier = 1L;
    }

    /**
     * Sets a pragma's value.
     * @param pragma The pragma to change.
     * @param value The value to set it to.
     */
    public void setPragma(Pragma pragma, String value) {
        pragmaTable.put(pragma.pragmaName, value);
    }

    private static final String[] OnOff = new String[] { "true", "false" };

    final static Set<String> pragmaSet = new TreeSet<String>();

    static {
        for (RQLiteConfig.Pragma pragma : RQLiteConfig.Pragma.values()) {
            pragmaSet.add(pragma.pragmaName);
        }
    }

    /**
     * Configures a connection.
     * @param conn The connection to configure.
     * @throws SQLException
     */
    public void apply(Connection conn) throws SQLException {

        HashSet<String> pragmaParams = new HashSet<String>();
        for (Pragma each : Pragma.values()) {
            pragmaParams.add(each.pragmaName);
        }

//        if(conn instanceof RQLiteConnection) {
//        	RQLiteConnection sqliteConn = (RQLiteConnection) conn;
//        }
        pragmaParams.remove(Pragma.USE_PASSWORD.pragmaName);


        Statement stat = conn.createStatement();
        try {
            
            for (Object each : pragmaTable.keySet()) {
                String key = each.toString();
                if (!pragmaParams.contains(key)) {
                    continue;
                }

                String value = pragmaTable.getProperty(key);
                if (value != null) {
                    stat.execute(String.format("pragma %s=%s", key, value));
                }
            }
        }
        finally {
            if (stat != null) {
                stat.close();
            }
        }

    }

    public static enum Pragma {
        USE_PASSWORD("use_password", OnOff);

        public final String   pragmaName;
        public final String[] choices;
        public final String   description;

        private Pragma(String pragmaName) {
            this(pragmaName, null);
        }

        private Pragma(String pragmaName, String[] choices) {
            this(pragmaName, null, choices);
        }

        private Pragma(String pragmaName, String description, String[] choices) {
            this.pragmaName = pragmaName;
            this.description = description;
            this.choices = choices;
        }

        public final String getPragmaName()
        {
            return pragmaName;
        }
    }

    /**
     * @return Array of DriverPropertyInfo objects.
     */
    static DriverPropertyInfo[] getDriverPropertyInfo() {
        Pragma[] pragma = Pragma.values();
        DriverPropertyInfo[] result = new DriverPropertyInfo[pragma.length];
        int index = 0;
        for (Pragma p : Pragma.values()) {
            DriverPropertyInfo di = new DriverPropertyInfo(p.pragmaName, null);
            di.choices = p.choices;
            di.description = p.description;
            di.required = false;
            result[index++] = di;
        }

        return result;
    }
}
