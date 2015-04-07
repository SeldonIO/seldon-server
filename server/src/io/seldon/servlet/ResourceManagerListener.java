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

package io.seldon.servlet;

import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.api.Util;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.caching.ClientIdCacheStore;
import io.seldon.api.service.DynamicParameterServer;
import io.seldon.api.service.async.JdoAsyncActionFactory;
import io.seldon.api.state.ZkAlgorithmUpdaterFactory;
import io.seldon.api.state.ZkCuratorHandler;
import io.seldon.api.state.ZkSubscriptionHandler;
import io.seldon.api.statsd.StatsdPeer;
import io.seldon.clustering.recommender.ClusterFromReferrerPeer;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.clustering.recommender.jdo.JdoUserDimCache;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.SecurityHashPeer;
import io.seldon.semvec.SemanticVectorsStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class ResourceManagerListener  implements ServletContextListener {

	private static Logger logger = Logger.getLogger( ResourceManagerListener.class.getName() );
	public static String baseDir = "";

    private ZkSubscriptionHandler zkSubHandler;

    public void contextInitialized(ServletContextEvent sce)
    {
        logger.info("**********************  STARTING API-SERVER INITIALISATION **********************");
		JDOFactory jdoFactory = null;
    	try
    	{  
    		final WebApplicationContext springContext = WebApplicationContextUtils.getWebApplicationContext(sce.getServletContext());
			jdoFactory = (JDOFactory) springContext.getBean("JDOFactory");

       
    		zkSubHandler =(ZkSubscriptionHandler) springContext.getBean("zkSubscriptionHandler");
    		//InputStream propStream = sce.getServletContext().getResourceAsStream("/WEB-INF/labs.properties");
    		InputStream propStream = getClass().getClassLoader().getResourceAsStream("/labs.properties");
    		SecurityHashPeer.initialise();
    		Properties props = new Properties();
    		props.load( propStream );
    	
    		// Set the default client name from properties if it exists
    		String defClientName = props.getProperty("io.seldon.labs.default.client");
    		if(defClientName !=null && defClientName.length() > 0) { Constants.DEFAULT_CLIENT = defClientName; }

    		CountRecommender.initialise(props);
    		
    		TestingUtils.initialise(props);
    		JdoCountRecommenderUtils.initialise(props);

    		MemCachePeer.initialise(props);
    		
    		baseDir = sce.getServletContext().getRealPath("/");
    		SemanticVectorsStore.initialise(props);
    		String dbPediaIndexPath = props.getProperty("dbpedia.index.path");
    		if (dbPediaIndexPath != null)
    		{
    			String useRamDirectoryStr = props.getProperty("dbpedia.index.ramdirectory");
    			boolean useRamDirectory = "true".equals(useRamDirectoryStr);
    		}
    		
    
    		String backend = props.getProperty("io.seldon.labs.backend");
    		String caching = props.getProperty("io.seldon.labs.caching");
    		if(caching !=null && caching.length() > 0) 
    		{ 
    			Constants.CACHING = Boolean.parseBoolean(caching); 
    			logger.warn("CACHING changed to "+Constants.CACHING);
    		}
    		if (backend != null)
    		{
    			// Mysql is default and only backend at present
    			//if ("mysql".equals(backend))
    			//{
    				Util.setBackEnd(Util.BackEnd.MYSQL);
    			//}
    		}
    		
    		//Initialise local cache of user and item ids
    		ClientIdCacheStore.initialise(props);
    		
    		//Initialise AsynActionQueue
    		JdoAsyncActionFactory.create(props);
    		AsyncClusterCountFactory.create(props);
    		
    		//Initialise Memory User Clusters (assumes JDO backend)
    		ClusterFromReferrerPeer.initialise(props);
    		

    		ActionHistoryCache.initalise(props);

    		DynamicParameterServer.startReloadTimer();
    		
    		StatsdPeer.initialise(props);
    		JdoUserDimCache.initialise(props);

    		ZkCuratorHandler curatorHandler = ZkCuratorHandler.getPeer();
    		if (curatorHandler != null)
    		{
    			ZkAlgorithmUpdaterFactory.initialise(props,curatorHandler);
    		}
    		
    		logger.info("**********************  ENDING API-SERVER INITIALISATION **********************");
    	}
    	catch( IOException ex )
    	{
             logger.error("IO Exception",ex);
    	}
    	catch (Exception ex)
    	{
    		logger.error("Exception at resource initialization",ex);
    	}
    	catch (Throwable e)
    	{
    		logger.error("Throwable during initialization ",e);
    	}
    	finally
        {
			if(jdoFactory!=null)
				jdoFactory.cleanupPM();
        }
    }
    
    public void contextDestroyed(ServletContextEvent sce)
    {
    	baseDir = "";
    	DynamicParameterServer.stopReloadTimer();
    	ClusterFromReferrerPeer.shutdown();
        if(MemCachePeer.getClient()!=null){
    	    MemCachePeer.getClient().shutdown();
        }
        try {
            ZkAlgorithmUpdaterFactory.destroy();
        } catch (InterruptedException e) {

        }
        ZkCuratorHandler.shutdown();
    }

}
