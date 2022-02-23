package com.webcomm.rqlite;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

public class RQLiteJDBCLoader {

    /**
     * @return The major version of the RQLite JDBC driver.
     */
    public static int getMajorVersion() {
        String[] c = getVersion().split("\\.");
        return (c.length > 0) ? Integer.parseInt(c[0]) : 1;
    }

    /**
     * @return The minor version of the RQLite JDBC driver.
     */
    public static int getMinorVersion() {
        String[] c = getVersion().split("\\.");
        return (c.length > 1) ? Integer.parseInt(c[1]) : 0;
    }

    /**
     * @return The version of the RQLite JDBC driver.
     */
    public static String getVersion() {

        URL versionFile = RQLiteJDBCLoader.class.getResource("rqlite-jdbc.properties");

        String version = "unknown";
        try {
            if(versionFile != null) {
                Properties versionData = new Properties();
                versionData.load(versionFile.openStream());
                version = versionData.getProperty("version", version);
                version = version.trim().replaceAll("[^0-9\\.]", "");
            }
        }
        catch(IOException e) {
            System.err.println(e);
        }
        return version;
    }
}
