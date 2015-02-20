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

package io.seldon.api.state.zk;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import io.seldon.api.state.*;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author firemanphil
 *         Date: 26/11/14
 *         Time: 13:51
 */
@Component
public class ZkClientConfigHandler implements PathChildrenCacheListener, GlobalConfigUpdateListener, ClientConfigHandler {
    private static Logger logger = Logger.getLogger(ZkClientConfigHandler.class.getName());
    private final ZkSubscriptionHandler handler;
    private final GlobalConfigHandler clientListHandler;
    private Set<String> clientSet;
    private final Set<ClientConfigUpdateListener> listeners;
    private static final String CLIENT_LIST_LOCATION = "clients";

    @Autowired
    public ZkClientConfigHandler(ZkSubscriptionHandler handler, GlobalConfigHandler clientListHandler) {
        this.handler = handler;
        this.clientListHandler = clientListHandler;
        this.listeners = new HashSet<>();
        this.clientSet = new HashSet<>();
    }

    @PostConstruct
    private void init(){
        logger.info("Initializing...");
        clientListHandler.addSubscriber(CLIENT_LIST_LOCATION, this);
    }


    @Override
    public void childEvent(CuratorFramework framework, PathChildrenCacheEvent event) throws Exception {

        switch (event.getType()){
            case CHILD_ADDED:
            case CHILD_UPDATED:
                String location = event.getData().getPath();
                for (String client : clientSet){
                    if(location.contains("/"+client+"/")){
                        location = location.replace("/"+client +"/","");
                        for(ClientConfigUpdateListener listener: listeners){
                            listener.configUpdated(client, location, new String(event.getData().getData()));
                        }
                    }
                }

                break;
            case CHILD_REMOVED:
                break;

        }
    }

    @Override
    public void configUpdated(String configKey, String configValue) {
        logger.info("Received new list of clients: " + configValue);
        // client added/removed
        if (configValue!=null){
            String[] clientsArray  = configValue.split(",");
            for (String client : clientsArray){
                if(!clientSet.contains(client)){
                    logger.info("Found new client in list : " + client);
                    try {
                        handler.addSubscription("/" + client, this);
                        doInitialRead(client);
                        clientSet.add(client);
                    }catch (Exception e){
                        logger.error("Couldn't add listener for client " + client, e);
                    }
                }
            }
        }


    }

    private void doInitialRead(String client) {
        Map<String, String> values = handler.getChildrenValues(client);
        for (Map.Entry<String, String> entry : values.entrySet()){
            for(ClientConfigUpdateListener listener : listeners){
                listener.configUpdated(client, entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void addListener(ClientConfigUpdateListener listener) {
        listeners.add(listener);
    }
}
