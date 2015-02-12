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

package io.seldon.mgm.keyword;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.seldon.api.Util;
import io.seldon.api.state.ZkCuratorHandler;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.EnsurePath;

import javax.annotation.PreDestroy;

@Component
public class ZkMgmKeywordConfUpdater implements PathChildrenCacheListener {

	private static Logger logger = Logger.getLogger(ZkMgmKeywordConfUpdater.class.getName());
	
	public static final String path = "mgmkeyword";
	public static final String endpointPath = "endpoint";
	public static final String confPath = "conf";
	
	PathChildrenCache cache;
	
	public void initialise(ZkCuratorHandler curator){
		try
		{
			CuratorFramework client = curator.getCurator();
			boolean ok = false;
			for(int attempts=0;attempts<3 && !ok;attempts++)
			{
				logger.info("Waiting until zookeeper connected");
				ok = client.getZookeeperClient().blockUntilConnectedOrTimedOut();
				if (ok)
					logger.info("zookeeper connected on attempt "+attempts);
				else
				{
					logger.error("Timed out waiting for zookeeper connect : attempt "+attempts);
				}
			}
			if (!ok)
			{
				logger.error("Failed to connect to zookeeper after multiple attempts - STOPPING");
				return;
			}
			cache = new PathChildrenCache(client, '/'+path, true);

			EnsurePath ensureMvTestPath = new EnsurePath('/' + path + "/" + endpointPath);
			ensureMvTestPath.ensure(client.getZookeeperClient());
			ensureMvTestPath = new EnsurePath('/' + path + "/" + confPath);
			ensureMvTestPath.ensure(client.getZookeeperClient());

			cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
			cache.getListenable().addListener(this);
			logger.info("Watching path " + path);
			for(ChildData data : cache.getCurrentData()){
				childEvent(client, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
			};
		}
		catch (Exception e)
		{
			logger.error("Zk initialise error ",e);
		}
	}

	public void childEventUpdate(String path,byte[] data) throws JsonParseException, JsonMappingException, IOException
	{
		 if(data.length > 0)
	        {
	            String type = path.split("/")[2];
	            if (endpointPath.equals(type))
	            {
	            	String endpoint = new String(data,"UTF-8");
	            	logger.info("Updating endpoint to "+endpoint);
	            	Util.getMgmKeywordConf().setEndpoint(endpoint);
	            }
	            else if (confPath.equals(type))
	            {
	            	logger.info("Conf Received "+new String(data)+ " of length " +data.length + " from path " + path);
	                ObjectMapper mapper = new ObjectMapper();
	                MgmKeywordConfBean b = mapper.readValue(data, MgmKeywordConfBean.class);
	                Util.getMgmKeywordConf().updateConf(b);
	            }
	        }
	}
	
	@Override
	public void childEvent(CuratorFramework arg0, PathChildrenCacheEvent event)
			throws Exception {
		if (event.getData() != null && event.getData().getData() != null)
		{
			byte[] data = event.getData().getData();
			String path = event.getData().getPath();
			childEventUpdate(path, data);
		}
		else
			logger.warn("No data from event "+event.getType().name()+" : "+event.toString());
	}
	

    @PreDestroy
    public void shutdown(){
        logger.info("Shutting down...");
        try {
            cache.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
