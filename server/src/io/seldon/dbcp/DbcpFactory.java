/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.dbcp;

import io.seldon.api.state.GlobalConfigHandler;
import io.seldon.api.state.GlobalConfigUpdateListener;
import io.seldon.db.jdo.JDOFactory;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.dbcp.AbandonedConfig;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Component
public class DbcpFactory implements DbcpPoolHandler,GlobalConfigUpdateListener {
	private static Logger logger = Logger.getLogger( DbcpFactory.class.getName() );

	private final GlobalConfigHandler globalConfigHandler;
	
	private final Map<String,DataSource> dataSources = new ConcurrentHashMap<String,DataSource>();
	List<DbcpInitialisedListener> listeners = new ArrayList<DbcpInitialisedListener>();
	boolean initialised = false;
	
	@Autowired
    public DbcpFactory(GlobalConfigHandler globalConfigHandler)
    {
    	this.globalConfigHandler = globalConfigHandler;
    	globalConfigHandler.addSubscriber("dbcp", this);
    }
	
	
	
	private void createC3p0(DbcpConfig conf) 
	{
		if (!dataSources.containsKey(conf.name))
		{
			try
			{
				logger.info("Creating c3p0 pool "+conf.toString());
				ComboPooledDataSource cpds = new ComboPooledDataSource();
				cpds.setDriverClass( conf.driverClassName ); //loads the jdbc driver            
				cpds.setJdbcUrl( conf.jdbc );
				cpds.setUser( conf.user);                                  
				cpds.setPassword( conf.password);                                  
				
				// the settings below are optional -- c3p0 can work with defaults
				cpds.setMinPoolSize(conf.maxIdle);                                     
				cpds.setAcquireIncrement(5);
				cpds.setMaxPoolSize(conf.maxActive);
				cpds.setPreferredTestQuery(conf.validationQuery);
				cpds.setNumHelperThreads(10);
			
				dataSources.put(conf.name, cpds);
			} catch (PropertyVetoException e) {
				logger.error("Failed to create c3p0 datasource ",e);
			}
			
		}
		else
		{
			logger.error("Pool "+conf.name+" already exists. Can't change existing datasource at present.");
		}
	}
    
	private void createDbcp(DbcpConfig conf)
	{
		if (!dataSources.containsKey(conf.name))
		{
			logger.info("Creating pool "+conf.toString());
			 // create a generic pool
		    GenericObjectPool pool = new GenericObjectPool(null);
		    pool.setMaxActive(conf.maxActive);
		    pool.setMaxIdle(conf.maxIdle);
		    pool.setMaxWait(conf.maxWait);
		    pool.setTimeBetweenEvictionRunsMillis(conf.timeBetweenEvictionRunsMillis);
		    pool.setMinEvictableIdleTimeMillis(conf.minEvictableIdleTimeMillis);
		    pool.setTestWhileIdle(conf.testWhileIdle);
		    pool.setTestOnBorrow(conf.testOnBorrow);
		    
		    AbandonedConfig abandondedConfig = new AbandonedConfig();
		    abandondedConfig.setRemoveAbandoned(conf.removeAbanadoned);
		    abandondedConfig.setRemoveAbandonedTimeout(conf.removeAbandonedTimeout);
		    abandondedConfig.setLogAbandoned(conf.logAbandonded);
		    
		    try
		    {
		    Class.forName(conf.driverClassName);
		    
		    DriverManagerConnectionFactory cf =  new DriverManagerConnectionFactory(conf.jdbc,conf.user,conf.password);
		    
		    PoolableConnectionFactory pcf  =  new PoolableConnectionFactory(cf, pool, null, conf.validationQuery, false, true,abandondedConfig);
		   
		    DataSource ds = new PoolingDataSource(pool);
		    dataSources.put(conf.name, ds);

		    } catch (ClassNotFoundException e) {
				logger.error("Failed to create datasource for "+conf.name+ " with class "+conf.driverClassName);
			}
		   
		}
		else
		{
			logger.error("Pool "+conf.name+" already exists. Can't change existing datasource at present.");
		}
	}

	private synchronized void updateInitialised()
	{
		if (!initialised)
		{
			this.initialised = true;
			for (DbcpInitialisedListener l : listeners)
				l.dbcpInitialised();
		}
	}
	
	@Override
	public synchronized void addInitialisedListener(DbcpInitialisedListener listener) {
		listeners.add(listener);
		if (initialised)
			for (DbcpInitialisedListener l : listeners)
				l.dbcpInitialised();
	}
	
	@Override
	public DataSource get(String name)
	{
		return dataSources.get(name);
	}
	
	@Override
	public void configUpdated(String configKey, String configValue) {
		configValue = StringUtils.strip(configValue);
        logger.info("KEY WAS " + configKey);
        logger.info("Received new dbcp settings: " + configValue);
        
        if (StringUtils.length(configValue) == 0) {
        	logger.warn("*WARNING* no dbcp is set!");
        } else {
            try {
            	logger.info("Processing configs "+configValue);
                ObjectMapper mapper = new ObjectMapper();
                DbcpConfigList configs = mapper.readValue(configValue, DbcpConfigList.class);
                
                for (DbcpConfig config : configs.dbs)
                	createDbcp(config);
                updateInitialised();
                logger.info("Successfully set dbcp.");
            } catch (IOException e){
                logger.error("Problem changing dbcp ", e);
            } 
        }
	}
	
	
	
	public static class DbcpConfigList {
		public List<DbcpConfig> dbs;
	}
	
	public static class DbcpConfig {
		public String name = JDOFactory.DEFAULT_DB_JNDI_NAME;
		public String jdbc = "jdbc:mysql:replication://localhost:3306,localhost:3306/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true";
		public String driverClassName = "com.mysql.jdbc.ReplicationDriver";
		public String user = "user1";
		public String password = "mypass";
		public Integer maxActive = 600;
		public Integer maxIdle = 5;
		public Integer maxWait = 20000;
		public Integer timeBetweenEvictionRunsMillis = 10000;
		public Integer minEvictableIdleTimeMillis = 60000;
		public Boolean testWhileIdle = true;
		public Boolean testOnBorrow = true;
		public String validationQuery = "/* ping */ SELECT 1";
		public Boolean removeAbanadoned = true;
		public Integer removeAbandonedTimeout = 60;
		public Boolean logAbandonded = false;
		@Override
		public String toString() {
			return "DbcpConfig [name=" + name + ", jdbc=" + jdbc
					+ ", driverClassName=" + driverClassName + ", user=" + user
					+ ", password=" + password + ", maxActive=" + maxActive
					+ ", maxIdle=" + maxIdle + ", maxWait=" + maxWait
					+ ", timeBetweenEvictionRunsMillis="
					+ timeBetweenEvictionRunsMillis
					+ ", minEvictableIdleTimeMillis="
					+ minEvictableIdleTimeMillis + ", testWhileIdle="
					+ testWhileIdle + ", testOnBorrow=" + testOnBorrow
					+ ", validationQuery=" + validationQuery
					+ ", removeAbanadoned=" + removeAbanadoned
					+ ", removeAbandonedTimeout=" + removeAbandonedTimeout
					+ ", logAbandonded=" + logAbandonded + "]";
		}
		
		
		
	}

	

	
}
