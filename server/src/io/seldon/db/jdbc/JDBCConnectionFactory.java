/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.db.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class JDBCConnectionFactory {

	private static Logger logger = Logger.getLogger( JDBCConnectionFactory.class.getName() );
	
	private static JDBCConnectionFactory factory;
	
	private Map<String,DataSource> dataSources = new ConcurrentHashMap<>();
	private Map<String,String> clientToCatalog = new ConcurrentHashMap<>();
	private InitialContext ctx;


	public JDBCConnectionFactory(){
		try {
			ctx = new InitialContext();
		} catch (NamingException e) {
			ctx = null;
			logger.error("Couldn't start JDNI context",e);
		}
		factory = this;
	}

//	public static JDBCConnectionFactory initialise( Map<String,String> jndiKeys,Map<String,String> clientToCatalog) throws NamingException
//	{
//		if (factory == null)
//			factory = new JDBCConnectionFactory();
//
//		for(Map.Entry<String, String> jndiKey : jndiKeys.entrySet())
//			factory.addDataSource(ctx, jndiKey.getKey(), jndiKey.getValue(),clientToCatalog.get(jndiKey.getKey()));
//
//		return factory;
//	}
	
	public static JDBCConnectionFactory get()
	{
		return factory;
	}
	
	public void addDataSource(String key,String jndi,String dbName) throws NamingException
	{
		 DataSource ds = (DataSource)ctx.lookup(jndi);
		 dataSources.put(key, ds);
		 if (dbName != null)
			 clientToCatalog.put(key, dbName);
		 else
			 clientToCatalog.put(key, key);
	}
	
	public Connection getConnection(String client) throws SQLException
	{
		return getConnection(client,false);
	}
	
	public Connection getConnection(String client,boolean readonly) throws SQLException
	{
		DataSource ds = dataSources.get(client);
		Connection c = null;
		if (ds != null)
		{
			c = ds.getConnection();
			if (readonly)
				c.setReadOnly(true);
			else
				c.setReadOnly(false);
			c.setCatalog(clientToCatalog.get(client));
		}
		else {
//			logger.error("Can't get connection for client "+client);
            final String message = "Can't get connection for client " + client;
            logger.error(message, new Exception(message));
        }
		return c;
	}
	
}
