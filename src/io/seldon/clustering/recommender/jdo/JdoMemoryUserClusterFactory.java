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

package io.seldon.clustering.recommender.jdo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jdo.PersistenceManager;

import io.seldon.clustering.recommender.UserCluster;
import org.apache.log4j.Logger;

import io.seldon.clustering.recommender.MemoryUserClusterStore;
import io.seldon.db.jdo.JDOFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class JdoMemoryUserClusterFactory {

	private static Logger logger = Logger.getLogger( JdoMemoryUserClusterFactory.class.getName() );
	private static final int RELOAD_INTERVAL_SECS = 300;
	private static final int RELOAD_INTERVAL_TRANS_SECS = 30;
	private ConcurrentHashMap<String,MemoryUserClusterStore> stores = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String,Long> lastTransientId = new ConcurrentHashMap<>();
	private ExecutorService service = Executors.newFixedThreadPool(2);
    private Set<Timer> timers = new HashSet<>();

    @Value("${io.seldon.memoryuserclusters.clients:}")
    private String clients;

	private static JdoMemoryUserClusterFactory factory;
    private List<String> clientsSplit = new ArrayList<>();

    public static JdoMemoryUserClusterFactory get()
	{
		return factory;
	}

    @PostConstruct
    public void initialise(){
        factory = this;
        if (clients != null)
        {
            clientsSplit = Arrays.asList(clients.split(","));
            for(String client : clientsSplit)
            {
                lastTransientId.put(client, -1L);
            }
        }
    }

    public static JdoMemoryUserClusterFactory initialise(Properties properties){
        JdoMemoryUserClusterFactory fact = new JdoMemoryUserClusterFactory();
        fact.clients = properties.getProperty("io.seldon.memoryuserclusters.clients");
        fact.initialise();
        return fact;
    }
	
	private void storeClusters(MemoryUserClusterStore store,List<UserCluster> clusters)
	{
		long currentUser = -1;
		List<UserCluster> userClusters = new ArrayList<>();
		for(UserCluster cluster : clusters)
		{
			if (currentUser != -1 && currentUser != cluster.getUser())
			{
				store.store(currentUser, userClusters);
				userClusters = new ArrayList<>();
			}
			userClusters.add(cluster);
			currentUser = cluster.getUser();
		}
		if (userClusters.size() > 0)
			store.store(currentUser, userClusters);
	}
	
	private synchronized void loadTransientClustersFromDB(String client)
	{
		PersistenceManager pm = JDOFactory.getPersistenceManager(client);
		if (pm != null)
		{
				JdoUserClusterStore jdoStore = new JdoUserClusterStore(pm);
				JdoUserClusterStore.TransientUserClusters tclusters = jdoStore.getTransientClusters(lastTransientId.get(client));
				if (tclusters != null && tclusters.clusters.size() > 0)
				{
					MemoryUserClusterStore store = stores.get(client);
					if (store != null)
					{
						logger.info("Loading "+tclusters.clusters.size()+" new transient clusters for "+client);
						storeClusters(store,tclusters.clusters);
						lastTransientId.put(client,tclusters.checkpoint);
					}
					else
						logger.warn("No store for "+client+" so can't load transient clusters");
				}
				else
					logger.debug("No new transient clusters to load for "+client);
		}
	}
	
	private void loadClustersFromDB(String client)
	{
		PersistenceManager pm = JDOFactory.getPersistenceManager(client);
		if (pm != null)
		{
			JdoUserClusterStore jdoStore = new JdoUserClusterStore(pm);
			logger.info("Loading user clusters for "+client);
            Set<Integer> userSet = new HashSet<>();
			List<UserCluster> clusters = jdoStore.getClusters();
            int userCount = countUsers(clusters);
			MemoryUserClusterStore store = new MemoryUserClusterStore(client,userCount);
			storeClusters(store,clusters);
			store.setLoaded(true);
			factory.store(client, store);
			
			//load transient clusters
			lastTransientId.put(client, -1L);
			loadTransientClustersFromDB(client);
			
			logger.info("Loaded user clusters for "+client);
		}
		else {
//			logger.error("Couldn't get om for client "+client+" so can't reload clusters");
            final String message = "Couldn't get om for client " + client + " so can't reload clusters";
            logger.error(message, new Exception(message));
        }
	}

    private int countUsers(List<UserCluster> clusters) {
        Set<Long> users = new HashSet<>();
        for (UserCluster cluster : clusters){
            users.add(cluster.getUser());
        }
        return users.size();
    }

    @Scheduled(initialDelay = 60 * 1000, fixedRate = RELOAD_INTERVAL_SECS * 1000)
	private void reload() {

        for (final String client : clientsSplit) {
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        logger.debug("Looking at possible reload of clusters for client " + client);
                        PersistenceManager pm = JDOFactory.getPersistenceManager(client);
                        if (pm != null) {
                            logger.debug("Getting jdo user cluster store " + client);
                            JdoUserClusterStore jdoStore = new JdoUserClusterStore(pm);
                            long currentTimestamp = jdoStore.getCurrentTimestamp();
                            MemoryUserClusterStore memoryUserClusterStore = stores.get(client);
                            long storeTimestamp = memoryUserClusterStore==null?
                                    0L :
                                    memoryUserClusterStore.getCurrentTimestamp();
                            if (currentTimestamp != storeTimestamp) {
                                logger.info("Reloading clusters for " + client + " as current timestamp is " + currentTimestamp + " while store has " + storeTimestamp);
                                long t1 = System.currentTimeMillis();
                                loadClustersFromDB(client);
                                long t2 = System.currentTimeMillis();
                                logger.info("Reload finished for " + client + " in " + (t2 - t1)+"msec");
                            } else
                                logger.debug("No Reload of  clusters for " + client + " as current timestamp is " + currentTimestamp + " and store has " + storeTimestamp);
                        } else {
//							logger.error("Can't get pm for client "+client+" so no reload done of clusters");
                            final String message = "Can't get pm for client " + client + " so no reload done of clusters";
                            logger.error(message, new Exception(message));
                        }
                    } catch (Exception e) {
                        logger.error("Exception while updating clusters", e);
                    } finally {
                        JDOFactory.cleanupPM();
                    }
                }
            });

        }
    }

    @Scheduled(initialDelay = 60 * 1000, fixedRate = RELOAD_INTERVAL_TRANS_SECS * 1000)
    private void reloadTransient()
	{
        for (final String client : clientsSplit) {
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        logger.debug("load of transient clusters for client " + client + " from checkpoint:" + lastTransientId.get(client));
                        loadTransientClustersFromDB(client);
                        logger.debug("Finished load of transient clusters for client " + client + " with new checkpoint:" + lastTransientId.get(client));
                    } catch (Exception e) {
                        logger.error("Caught exception trying to load transient clusters", e);
                    } finally {
                        JDOFactory.cleanupPM();
                    }
                }
            });
        }
	}

    @PreDestroy
    public void tearDown(){
        logger.info("Shutting down JdoMemoryUserClusterFactory...");
        service.shutdown();

    }

	public void store(String client,MemoryUserClusterStore store)
	{
		stores.put(client, store);
	}
	
	public MemoryUserClusterStore get(String client)
	{
		return stores.get(client);
	}
}
