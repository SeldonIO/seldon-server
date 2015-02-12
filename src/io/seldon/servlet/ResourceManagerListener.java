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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import io.seldon.api.Util;
import io.seldon.api.state.ZkCuratorHandler;
import io.seldon.clustering.recommender.jdo.JdoUserDimCache;
import io.seldon.db.jdo.servlet.JDOStartup;
import io.seldon.mgm.keyword.ZkMgmKeywordConfUpdater;
import io.seldon.nlp.StopWordPeer;
import io.seldon.trust.offline.TrustGraphUpdateExecutor;
import org.apache.log4j.Logger;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.caching.ClientIdCacheStore;
import io.seldon.api.service.DynamicParameterServer;
import io.seldon.api.service.async.JdoAsyncActionFactory;
import io.seldon.api.state.ZkAlgorithmUpdaterFactory;
import io.seldon.api.state.ZkMgmUpdater;
import io.seldon.api.state.ZkParameterUpdater;
import io.seldon.api.state.ZkSubscriptionHandler;
import io.seldon.api.statsd.StatsdPeer;
import io.seldon.clustering.minhash.MinHashClusterPeer;
import io.seldon.clustering.recommender.ClusterFromReferrerPeer;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.GlobalWeightedMostPopular;
import io.seldon.clustering.recommender.MemcacheClusterCountFactory;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.clustering.tag.AsyncTagClusterCountFactory;
import io.seldon.cooc.CooccurrencePeerFactory;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.facebook.importer.FacebookOnlineImporterConfiguration;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestOutputResultsTimer;
import io.seldon.graphlab.GraphLabRecommenderStore;
import io.seldon.mahout.ALSRecommenderStore;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.SecurityHashPeer;
import io.seldon.realtime.ActionProcessorPeer;
import io.seldon.semvec.SemanticVectorsStore;
import io.seldon.similarity.dbpedia.DBpediaVectorStore;
import io.seldon.similarity.dbpedia.jdo.SqlWebSimilaritySimplePeer;
import io.seldon.storm.DRPCSettingsFactory;

public class ResourceManagerListener  implements ServletContextListener {

	private static Logger logger = Logger.getLogger( ResourceManagerListener.class.getName() );
	public static String baseDir = "";
	static ZkMgmKeywordConfUpdater mgmKeywordConfUpdater;

    private ZkMgmUpdater mgmMultivariateTestUpdater;
    private MultiVariateTestOutputResultsTimer mvTestLogTimer;
    private ZkParameterUpdater parameterUpdater;
    private ZkSubscriptionHandler zkSubHandler;

    public void contextInitialized(ServletContextEvent sce)
    {
        logger.info("**********************  STARTING API-SERVER INITIALISATION **********************");
    	try
    	{  
    		final WebApplicationContext springContext = WebApplicationContextUtils.getWebApplicationContext(sce.getServletContext());
            mgmMultivariateTestUpdater =
                    (ZkMgmUpdater)springContext.getBean("zkMgmUpdater");
            parameterUpdater = (ZkParameterUpdater) springContext.getBean("zkParameterUpdater");
            mvTestLogTimer = (MultiVariateTestOutputResultsTimer) springContext.getBean("multiVariateTestOutputResultsTimer");
            mgmKeywordConfUpdater = (ZkMgmKeywordConfUpdater)springContext.getBean("zkMgmKeywordConfUpdater");
    		zkSubHandler =(ZkSubscriptionHandler) springContext.getBean("zkSubscriptionHandler");
    		//InputStream propStream = sce.getServletContext().getResourceAsStream("/WEB-INF/labs.properties");
    		InputStream propStream = getClass().getClassLoader().getResourceAsStream("/labs.properties");
    		SecurityHashPeer.initialise();
    		Properties props = new Properties();
    		props.load( propStream );
    	
    		// Set the default client name from properties if it exists
    		String defClientName = props.getProperty("io.seldon.labs.default.client");
    		if(defClientName !=null && defClientName.length() > 0) { Constants.DEFAULT_CLIENT = defClientName; }

    		FacebookOnlineImporterConfiguration.initialise(props);
    		CountRecommender.initialise(props);
    		
    		TestingUtils.initialise(props);
    		JdoCountRecommenderUtils.initialise(props);
    		
    		JDOStartup.contextInitialized(sce);
    		MemCachePeer.initialise(props);
    		
    		TrustGraphUpdateExecutor.initialise();
    		baseDir = sce.getServletContext().getRealPath("/");
    		SemanticVectorsStore.initialise(props);
    		String dbPediaIndexPath = props.getProperty("dbpedia.index.path");
    		if (dbPediaIndexPath != null)
    		{
    			String useRamDirectoryStr = props.getProperty("dbpedia.index.ramdirectory");
    			boolean useRamDirectory = "true".equals(useRamDirectoryStr);
    		}
    		String dbPediaVectorsPath = props.getProperty("dbpedia.semvectors.terms");
    		if (dbPediaVectorsPath != null)
    			DBpediaVectorStore.initialise(dbPediaVectorsPath);
    		StopWordPeer.initialise(sce.getServletContext().getRealPath("/WEB-INF/nlp/stopwords.txt"));
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
    		AsyncTagClusterCountFactory.create(props);
    		
    		//Initialise Memory User Clusters (assumes JDO backend)
    		ClusterFromReferrerPeer.initialise(props);
//    		JdoMemoryUserClusterFactory.initialise(props);
    		//Initialise Memory Cluster Counter
    		MemoryClusterCountFactory.create(props);
    		MemcacheClusterCountFactory.create(props);
    		GlobalWeightedMostPopular.initialise(props);
    		
    		ActionHistoryCache.initalise(props);
    		
    		//Mahout ALS
    		String alsClients = props.getProperty("io.seldon.mahout.als.clients");
    		String alsBase = props.getProperty("io.seldon.mahout.als.dir");
    		if (alsClients != null && alsBase != null)
    			ALSRecommenderStore.load(alsClients, alsBase);
    		
    		//Graphlab PMF
    		String graphlabClients = props.getProperty("io.seldon.graphlab.clients");
    		String graphlabBase = props.getProperty("io.seldon.graphlab.dir");
    		if (graphlabClients != null && graphlabBase != null)
    			GraphLabRecommenderStore.load(graphlabClients, graphlabBase);
    		
    		//MinHash fast user matching
    		String minHashActive = props.getProperty("io.seldon.minhash.active");
    		if ("true".equals(minHashActive))
    			MinHashClusterPeer.initialise(props);
    		
    		SqlWebSimilaritySimplePeer.initialise(props);

    		DynamicParameterServer.startReloadTimer();
    		
    		ActionProcessorPeer.initialise(props);
    		
    		CooccurrencePeerFactory.initialise(props);
    		
    		DRPCSettingsFactory.initialise(props);
    		
    		StatsdPeer.initialise(props);
    		JdoUserDimCache.initialise(props);

    		ZkCuratorHandler curatorHandler = ZkCuratorHandler.getPeer();
    		if (curatorHandler != null)
    		{
    			ZkAlgorithmUpdaterFactory.initialise(props,curatorHandler);
    			parameterUpdater.initialise(curatorHandler);
    			mgmKeywordConfUpdater.initialise(curatorHandler);
                mgmMultivariateTestUpdater.initialise(curatorHandler);
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
            JDOFactory.cleanupPM();
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
        mgmMultivariateTestUpdater.shutdown();
        ZkCuratorHandler.shutdown();
    }

}
