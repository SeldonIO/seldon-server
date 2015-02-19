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

package io.seldon.api.state;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.seldon.clustering.recommender.ClientClusterTypeService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.EnsurePath;

@Component
public class ZkParameterUpdater implements PathChildrenCacheListener {

	private static Logger logger = Logger.getLogger(ZkParameterUpdater.class.getName());
	
	private static final String CLIENTS_PARAM = "/clients/parameters";
	private static final String PARAM_CLUSTER = "clustertype";
	
	@Autowired
	private ClientClusterTypeService clusterTypeService;

	private Set<PathChildrenCache> caches = new HashSet<>();
	
	 public void initialise(ZkCuratorHandler curator){
	        logger.info("Starting zookeeper parameter server");
	        setupAndListenToExistingClients(curator);
	 }
	 
	 public void setupAndListenToExistingClients(ZkCuratorHandler curator)
	    {
	        logger.info("Setting listeners for mgm clients");
	        try
	        {
	            CuratorFramework curatorFramework = curator.getCurator();
	            for (String clientName : curatorFramework.getChildren().forPath(CLIENTS_PARAM))
	            {
	                PathChildrenCache cache = new PathChildrenCache(curatorFramework, CLIENTS_PARAM + '/' + clientName, true);
	                caches.add(cache);
	                String[] params = { PARAM_CLUSTER };
	                for (String type : params)
	                {
	                    EnsurePath ensureMvTestPath = new EnsurePath(CLIENTS_PARAM + '/' + clientName + '/' + type);
	                    ensureMvTestPath.ensure(curatorFramework.getZookeeperClient());
	                }
	                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
	                cache.getListenable().addListener(this);
	                logger.info("Watching path " + CLIENTS_PARAM + '/' + clientName);

	                for(ChildData data : cache.getCurrentData())
	                {
	                    childEvent(curatorFramework, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
	                }
	            }
	        }
	        catch (Exception e)
	        {
	            logger.warn("Couldn't start listeners for path " + CLIENTS_PARAM, e);
	        }

	    }

	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
			throws Exception {
		if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED))
        {
            String path = event.getData().getPath();
            logger.info("CHILD_ADDED for " + path);
            updateParameters(client, event);
        } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) 
        {
            String path = event.getData().getPath();
            logger.info("CHILD_UPDATED for " + path);
            updateParameters(client, event);
                
        } else 
        {
            if (event != null) {
                logger.warn("Child event received for type " + event.getType() + " with data " + event.getData());
            }
        }

		
	}

	private void updateParameters(CuratorFramework framework, PathChildrenCacheEvent event)
	{
		final String path = event.getData().getPath();
		if (path.startsWith(CLIENTS_PARAM))
		{
			String[] parts = path.split("/");
			if (parts.length == 5)
			{
				String client = parts[3];
				String parameter = parts[4];
				final byte[] json = event.getData().getData();
				logger.info("client: "+client+ " parmeter: "+parameter+" data:"+new String(json)+" of length "+json.length);
				if (PARAM_CLUSTER.equals(parameter))
				{
					ObjectMapper mapper = new ObjectMapper();
	                try 
	                {
	                	String jsonStr = new String(json);
	                	ClusterTypeDefn clusterTypeMessage = mapper.readValue(json, ClusterTypeDefn.class);
						logger.info("Adding new types for client "+client);
						clusterTypeService.addTypes(client, new HashSet<>(clusterTypeMessage.types));
					} catch (Exception e) 
					{
						logger.error("Failed to map json to ClusterTypeDefn " + json.toString());
					}
				}
			}
			else
			{
				logger.error("Bad path with incorrect number of levels : "+path);
			}
				
		}
		else
		{
			logger.error("Unknown path - ignoring "+path);
		}
	}

	public static class ClusterTypeDefn
	{
		public List<Integer> types;
	}
}
