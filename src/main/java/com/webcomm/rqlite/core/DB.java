package com.webcomm.rqlite.core;

import java.net.URL;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.Gson;
import com.rqlite.NodeUnavailableException;
import com.rqlite.Rqlite;
import com.rqlite.RqliteFactory;
import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.QueryResults;
import com.webcomm.rqlite.RQLiteConfig;

public class DB {

    private final String url;
    private final RQLiteConfig config;
    private final AtomicBoolean closed = new AtomicBoolean(true);
    
    private final Rqlite rqlite;

    public DB(String url, RQLiteConfig config)
            throws Exception
    {
        this.url = url;
        this.config = config;
        URL aURL = new URL(url);
    	// System.out.println(url);
        this.rqlite = RqliteFactory.connect(aURL.getProtocol(), aURL.getHost(), aURL.getPort());
    }

    public final synchronized ExecuteResults executeUpdate(String q) throws SQLException, NodeUnavailableException {

		// System.out.println("executeUpdate " + q);
		
    	return rqlite.Execute(q);
    }

    public final synchronized ExecuteResults executeUpdate(Object[] q) throws SQLException, NodeUnavailableException {

		Gson gson = new Gson();
		// System.out.println("executeUpdate " + gson.toJson(q));
		
    	return rqlite.Execute(q, true);
    }
    
    public final synchronized QueryResults executeQuery(String q) throws SQLException, NodeUnavailableException {

		// System.out.println("executeQuery " + q);
		
    	return rqlite.Query(q, Rqlite.ReadConsistencyLevel.WEAK);
    }

    public final synchronized QueryResults executeQuery(Object[] q) throws SQLException, NodeUnavailableException {

		Gson gson = new Gson();
		// System.out.println("executeQuery " + gson.toJson(q));
		
    	return rqlite.Query(q, true, Rqlite.ReadConsistencyLevel.WEAK);
    }

    public String getUrl() {
        return url;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public RQLiteConfig getConfig() {
        return config;
    }
    
    public final synchronized void open() throws SQLException {
        closed.set(false);
    }
    public final synchronized void close() throws SQLException {
        closed.set(true);
    }
}
