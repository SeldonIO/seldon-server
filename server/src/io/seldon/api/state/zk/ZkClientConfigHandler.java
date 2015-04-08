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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.util.JSONParseException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import io.seldon.api.state.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

/**
 * @author firemanphil
 *         Date: 26/11/14
 *         Time: 13:51
 */
@Component
public class ZkClientConfigHandler implements TreeCacheListener, GlobalConfigUpdateListener, ClientConfigHandler {
    private static Logger logger = Logger.getLogger(ZkClientConfigHandler.class.getName());
    private final ZkSubscriptionHandler handler;
    private Map<String,Map<String,String>> clientsWithInitialConfig;
    private final Set<ClientConfigUpdateListener> listeners;
    private final Set<NewClientListener> newClientListeners;
    private static final String CLIENT_LIST_LOCATION = "all_clients";
    private ObjectMapper jsonMapper = new ObjectMapper();

    @Autowired
    public ZkClientConfigHandler(ZkSubscriptionHandler handler, GlobalConfigHandler clientListHandler) {
        this.handler = handler;
        this.newClientListeners = new HashSet<>();
        this.listeners = new HashSet<>();
        this.clientsWithInitialConfig = new HashMap<>();
    }

    @PostConstruct
    private void init() throws Exception {
        logger.info("Initializing...");
        handler.addSubscription("/" + CLIENT_LIST_LOCATION, this);
        Collection<ChildData> children = handler.getChildren("/" + CLIENT_LIST_LOCATION);
        logger.info("Found " +children.size() + " children under " +CLIENT_LIST_LOCATION );
        for(ChildData child : children)
            childEvent(null, new TreeCacheEvent(TreeCacheEvent.Type.NODE_ADDED,child));

    }

    private boolean isClientPath(String path) {
        // i.e. a path like /all_clients/testclient is one but /all_clients/testclient/mf is not
        return StringUtils.countMatches(path,"/") == 2;
    }

    @Override
    public void configUpdated(String configKey, String configValue) {
        logger.info("Received new list of clients: " + configValue);
        // client added/removed
        if (configValue!=null){
            String[] clientsArray  = configValue.split(",");
            for (String client : clientsArray){
                if(!clientsWithInitialConfig.keySet().contains(client)){
                    logger.info("Found new client in list : " + client);
                    try {
                        handler.addSubscription("/" + client, this);
                        clientsWithInitialConfig.keySet().add(client);
                    }catch (Exception e){
                        logger.error("Couldn't add listener for client " + client, e);
                    }
                }
            }
        }


    }

    private void doInitialRead(String client, ClientConfigUpdateListener listener) {
        if(!listeners.isEmpty()) {
            Map<String, String> values = handler.getChildrenValues("/" +CLIENT_LIST_LOCATION+"/"+client);
            for (Map.Entry<String, String> entry : values.entrySet()) {
                listener.configUpdated(client, StringUtils.remove(entry.getKey(),"/"+client+"/"), entry.getValue());
            }
        }
    }

    @Override
    public Map<String, String> requestCacheDump(String client){
        return handler.getChildrenValues("/" +CLIENT_LIST_LOCATION+"/"+client);
    }

    @Override
    public void addListener(ClientConfigUpdateListener listener, boolean notifyOnExistingData) {
        logger.info("Adding client config listener, current clients are " + StringUtils.join(clientsWithInitialConfig.keySet(),','));
        listeners.add(listener);
        if(notifyOnExistingData) {
            for (String client : clientsWithInitialConfig.keySet())
                doInitialRead(client,listener);
        }
    }

    @Override
    public void addNewClientListener(NewClientListener listener, boolean notifyExistingClients) {
        newClientListeners.add(listener);
        if(notifyExistingClients){
            for(String client : clientsWithInitialConfig.keySet())
                listener.clientAdded(client, clientsWithInitialConfig.get(client));
        }
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        if(event == null || event.getType() == null || event.getData() == null || event.getData().getPath()==null) {
            logger.warn("Event received was null somewhere");
            return;
        }
        logger.info("Received a message of type " + event.getType() + " at " + event.getData().getPath());
        switch (event.getType()){
            case NODE_ADDED:
                String path = event.getData().getPath();
                if(isClientPath(path)){
                    String clientName = path.replace("/"+CLIENT_LIST_LOCATION+"/","");
                    logger.info("Found new client : " + clientName);
                    Map<String, String> initialConfig = new HashMap<>();
                    try {
                         initialConfig = jsonMapper.readValue(
                                event.getData().getData(), new TypeReference<Map<String, String>>() {
                                });

                    } catch (IOException  e){

                        logger.warn("Couldn't read JSON at " + path + ", ignoring");
                    }
                    clientsWithInitialConfig.put(clientName, initialConfig);
                    for (NewClientListener listener : newClientListeners) {
                        listener.clientAdded(clientName, initialConfig);
                    }
                    break;
                } //purposeful cascade as the below deals with the rest of the cases

            case NODE_UPDATED:
                String location = event.getData().getPath();
                boolean foundAMatch = false;
                String[] clientAndNode = location.replace("/"+CLIENT_LIST_LOCATION +"/","").split("/");
                if(clientAndNode !=null && clientAndNode.length==2){
                    for(ClientConfigUpdateListener listener: listeners){
                        foundAMatch = true;
                        listener.configUpdated(clientAndNode[0], clientAndNode[1], new String(event.getData().getData()));
                    }

                } else {
                    logger.warn("Couldn't process message for node : " + location + " data : " + event.getData().getData());
                }
                if (!foundAMatch)
                    logger.warn("Received message for node " + location +" but found no interested listeners");

                break;
            case NODE_REMOVED:
                path = event.getData().getPath();
                if(isClientPath(path)){
                    String clientName = path.replace("/"+CLIENT_LIST_LOCATION+"/","");
                    clientsWithInitialConfig.keySet().remove(clientName);
                    logger.info("Deleted client : " + clientName);
                    for (NewClientListener listener: newClientListeners)
                        listener.clientDeleted(clientName);
                }

        }
    }
}
