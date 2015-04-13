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

import com.google.common.base.Joiner;
import io.seldon.api.state.*;
import io.seldon.mf.PerClientExternalLocationListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Provides notifications when a new resource is created.
 *
 * @author firemanphil
 *         Date: 07/10/2014
 *         Time: 18:39
 */
@Component
public class NewResourceNotifier implements ClientConfigUpdateListener {


    private static Logger logger = Logger.getLogger(NewResourceNotifier.class.getName());
    private ConcurrentHashMap<String, Map<String,PerClientExternalLocationListener>> nodeWatches = new ConcurrentHashMap<>();


    private final GlobalConfigHandler globalConfigHandler;
    private final ClientConfigHandler clientConfigHandler;

    @Autowired
    public NewResourceNotifier(GlobalConfigHandler globalConfigHandler, ClientConfigHandler clientConfigHandler) {
        this.globalConfigHandler = globalConfigHandler;
        this.clientConfigHandler = clientConfigHandler;
    }

    /**
     * Add a listener for all clients for this resource
     * @param nodePattern
     * @param listener
     */
    public void addListener(String nodePattern, PerClientExternalLocationListener listener){
        logger.info("Adding listener for new resource pattern "+ nodePattern);
        nodeWatches.put(nodePattern, new HashMap<String, PerClientExternalLocationListener>());
        globalConfigHandler.addSubscriber(nodePattern, createAllClientListeners(nodePattern, listener));
        clientConfigHandler.addListener(this, false);
    }


    private GlobalConfigUpdateListener createAllClientListeners(final String nodePattern, final PerClientExternalLocationListener listener) {
        return new GlobalConfigUpdateListener() {
            @Override
            public void configUpdated(String configKey, String configValue) {

                if(configValue == null)
                    return;
                String[] clients = configValue.split(",");
                Set<String> watchingClients = nodeWatches.get(nodePattern).keySet();
                Set<String> clientsSet = new HashSet<>(Arrays.asList(clients));
                Collection<String> toRemove = CollectionUtils.subtract(watchingClients, clientsSet);
                Collection<String> toAdd = CollectionUtils.subtract(clientsSet,watchingClients);
                for (String toRemoveString : toRemove){
                    logger.info("Remove subscription for client " + toRemoveString + " nodePattern " + nodePattern);
                    nodeWatches.get(nodePattern).remove(toRemoveString);
                    listener.clientLocationDeleted(toRemoveString,nodePattern);
                }
                for(final String client : toAdd){
                    nodeWatches.get(nodePattern).put(client, listener);

                    if(StringUtils.isNotEmpty(client)){
                        try {
                            logger.info("Add subscription for client " + client + " nodePattern " + nodePattern);
                            for(Map.Entry<String, String > entry : clientConfigHandler.requestCacheDump(client).entrySet()){
                                if(entry.getKey().equals(nodePattern))
                                    NewResourceNotifier.this.configUpdated(client, entry.getKey(), entry.getValue());
                            }
                        } catch (Exception e) {
                            logger.error("Problem when notifying about new resource location ",e);
                        }

                    }
                }
            }
        };
    }

    @Override
    public void configUpdated(String client, String configKey, String configValue) {

        logger.info("Received new config " + client + " "+ configKey +  " " + configValue + " available watches "+ StringUtils.join(nodeWatches.keySet()) + " available clients "+ Joiner.on('\n').withKeyValueSeparator(" -> ").join(nodeWatches));;
        Map<String, PerClientExternalLocationListener> watchingClients = nodeWatches.get(configKey);
        if(watchingClients!=null && watchingClients.containsKey(client)){
            watchingClients.get(client).newClientLocation(client,configValue,configKey);
        }
    }
}
