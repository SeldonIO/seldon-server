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

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.*;
import com.netflix.curator.utils.EnsurePath;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

/**
 * @author firemanphil
 *         Date: 06/10/2014
 *         Time: 16:10
 */
@Component
public class ZkSubscriptionHandler {
    private static Logger logger = Logger.getLogger(ZkSubscriptionHandler.class.getName());
    @Autowired
    private ZkCuratorHandler curator;

    private Map<String, PathChildrenCache> caches = new HashMap<>();
    private Map<String, NodeCache> nodeCaches = new HashMap<>();

    public void addSubscription(String location, PathChildrenCacheListener listener) throws Exception {
        CuratorFramework client = curator.getCurator();
        PathChildrenCache cache = new PathChildrenCache(client, location, true);
        caches.put(location, cache);
        EnsurePath ensureMvTestPath = new EnsurePath(location);
        ensureMvTestPath.ensure(client.getZookeeperClient());
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        cache.getListenable().addListener(listener);
        logger.info("Added ZooKeeper subscriber for " + location + " children.");
        for(ChildData data : cache.getCurrentData())
        {
            listener.childEvent(client, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
        }
    }

    public String getValue(String node) {
        if (nodeCaches.containsKey(node)) {
            return new String(nodeCaches.get(node).getCurrentData().getData());
        } else {
            return null;
        }
    }

    public Map<String, String> getChildrenValues(String node){
        Map<String, String> values = new HashMap<>();
        if(caches.containsKey(node)){
            List<ChildData> currentData = caches.get(node).getCurrentData();
            for(ChildData data : currentData)
            {
                values.put(StringUtils.replace(data.getPath(), "/" + node + "/", ""), new String(data.getData()));
            }
        }
        return values;
    }

    public boolean addSubscription(final String node, final ZkNodeChangeListener listener) {
        try {
            CuratorFramework client = curator.getCurator();
            final NodeCache cache = new NodeCache(client, node);
            nodeCaches.put(node, cache);
            cache.start(true);
            EnsurePath ensureMvTestPath = new EnsurePath(node);
            ensureMvTestPath.ensure(client.getZookeeperClient());

            logger.info("Added ZooKeeper subscriber for " + node);
            cache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    ChildData currentData = cache.getCurrentData();
                    if (currentData == null) {
                        listener.nodeDeleted(node);
                    } else {
                        String data = new String(currentData.getData());
                        listener.nodeChanged(node, data);
                    }

                }
            });
            ChildData data = cache.getCurrentData();
            if(data!=null && data.getData()!=null)
                listener.nodeChanged(node, new String(data.getData()));

            return true;
        } catch (Exception e){
            logger.error("Couldn't add subscription for "+node, e);
            return false;
        }
    }

    public void removeSubscription(final String node){
        NodeCache cache = nodeCaches.get(node);
        if(cache!=null){
            try {
                cache.close();
            } catch (IOException e) {
                logger.warn("Problem when removing zookeeper subscription for ");
            }
        }
    }


    @PreDestroy
    public void shutdown() throws IOException {
        for (PathChildrenCache cache : caches.values()){
            cache.close();
        }
        for (NodeCache cache : nodeCaches.values()){
            cache.close();
        }
    }




}
