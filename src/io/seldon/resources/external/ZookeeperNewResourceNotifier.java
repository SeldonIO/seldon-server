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

package io.seldon.resources.external;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.seldon.api.state.ZkNodeChangeListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.seldon.api.state.ZkSubscriptionHandler;
import io.seldon.mf.PerClientExternalLocationListener;

/**
 *
 * Provides a notification when a new resource is created and the notification
 * is sent via zookeeper.
 * @author firemanphil
 *         Date: 07/10/2014
 *         Time: 11:11
 */

@Component
public class ZookeeperNewResourceNotifier implements NewResourceNotifier {

    private static Logger logger = Logger.getLogger(ZookeeperNewResourceNotifier.class.getName());


    private final ZkSubscriptionHandler zkSubHander;
    private ConcurrentHashMap<String, Map<String,ZkNodeChangeListener>> clientWatches = new ConcurrentHashMap<>();

    @Autowired
    public ZookeeperNewResourceNotifier(ZkSubscriptionHandler zkSubHander) {
        this.zkSubHander = zkSubHander;
    }

    public void addListener(final String nodePattern, final PerClientExternalLocationListener listener){
        try {
            zkSubHander.addSubscription("/clients/"+nodePattern, createAllClientListeners(nodePattern, listener));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    

    private ZkNodeChangeListener createAllClientListeners(final String nodePattern, final PerClientExternalLocationListener listener) {
        return new ZkNodeChangeListener() {
            @Override
            public void nodeChanged(String node, String value) {
                if(value == null)
                    return;
                clientWatches.putIfAbsent(nodePattern, new ConcurrentHashMap<String,ZkNodeChangeListener>());
                String[] clients = value.split(",");
                Set<String> clientsSet = new HashSet<>(Arrays.asList(clients));
                Map<String,ZkNodeChangeListener> clientMap = clientWatches.get(nodePattern);
                Collection<String> toRemove = CollectionUtils.subtract(clientMap.keySet(),clientsSet);
                for (String toRemoveString : toRemove){
                	logger.info("Remove subscription for client "+toRemoveString+" nodePattern "+nodePattern);
                    zkSubHander.removeSubscription("/"+toRemoveString+"/"+nodePattern);
                    clientMap.remove(toRemoveString);
                    listener.clientLocationDeleted(toRemoveString,nodePattern);
                }
                for(final String client : value.split(",")){

                    if(StringUtils.isNotEmpty(client) && !clientMap.containsKey(client)){
                        try {
                        	logger.info("Add subscription for client "+client+" nodePattern "+nodePattern);
                            ZkNodeChangeListener changeListener = createClientNewResourceListener(client, listener,nodePattern);
                            zkSubHander.addSubscription("/"+client+"/"+nodePattern, changeListener);
                            clientMap.put(client, changeListener);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
            }

            @Override
            public void nodeDeleted(String node) {
                // This should never happen
            }
        };
    }

    private ZkNodeChangeListener createClientNewResourceListener(final String client, final PerClientExternalLocationListener listener,final String nodePattern) {
        return new ZkNodeChangeListener() {
                                    @Override
                                    public void nodeChanged(String node, String value) {
                                        if(StringUtils.isNotEmpty(value))
                                            listener.newClientLocation(client, value, nodePattern);
                                    }

                                    @Override
                                    public void nodeDeleted(String node) {
                                    	Map<String,ZkNodeChangeListener> clientMap = clientWatches.get(nodePattern);
                                        clientMap.remove(client);
                                        listener.clientLocationDeleted(client,nodePattern);
                                    }

                                };
    }
}
