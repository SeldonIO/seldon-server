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

import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;
import io.seldon.api.state.NewClientListener;
import io.seldon.api.state.ZkSubscriptionHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author firemanphil
 *         Date: 26/11/14
 *         Time: 13:51
 */
@Component
public class ZkClientConfigHandler implements TreeCacheListener, ClientConfigHandler {
    private static Logger logger = Logger.getLogger(ZkClientConfigHandler.class.getName());
    private final ZkSubscriptionHandler handler;
    private Map<String,Map<String,String>> clientsWithInitialConfig;
    private final Set<ClientConfigUpdateListener> listeners;
    private final ArrayList<NewClientListener> newClientListeners;
    private static final String CLIENT_LIST_LOCATION = "all_clients";
    private ObjectMapper jsonMapper = new ObjectMapper();

    private boolean initalized = false;


    @Autowired
    public ZkClientConfigHandler(ZkSubscriptionHandler handler){
        this.handler = handler;
        this.newClientListeners = new ArrayList<>();
        this.listeners = new HashSet<>();
        this.clientsWithInitialConfig = new ConcurrentHashMap<>();
    }

    private boolean isClientPath(String path) {
        // i.e. a path like /all_clients/testclient is one but /all_clients/testclient/mf is not
        return StringUtils.countMatches(path,"/") == 2;
    }

    @Override
    public Map<String, String> requestCacheDump(String client){
        if(initalized) {
            return handler.getChildrenValues("/" + CLIENT_LIST_LOCATION + "/" + client);
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public synchronized void addListener(ClientConfigUpdateListener listener) {
        logger.info("Adding client config listener, current clients are " + StringUtils.join(clientsWithInitialConfig.keySet(),','));
        listeners.add(listener);
    }

    @Override
    public synchronized void addNewClientListener(NewClientListener listener, boolean notifyExistingClients) {
        newClientListeners.add(listener);
        if(notifyExistingClients){
            for(String client : clientsWithInitialConfig.keySet())
                listener.clientAdded(client, clientsWithInitialConfig.get(client));
        }
    }

    @Override
    public synchronized void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        if(event == null || event.getType() == null) {
            logger.warn("Event received was null somewhere");
            return;
        }

        if(!initalized && event.getType() != TreeCacheEvent.Type.INITIALIZED) {
            logger.debug("Ignore event as we are not in an initialised state: " + event);
            return;
        }
        if(event.getType()== TreeCacheEvent.Type.NODE_ADDED || event.getType() == TreeCacheEvent.Type.NODE_UPDATED)
            logger.info("Message received from ZK : " + event.toString());
        switch (event.getType()){
            case NODE_ADDED:
                if( event.getData() == null || event.getData().getPath()==null) {
                    logger.warn("Event received was null somewhere");
                    return;
                }
                String path = event.getData().getPath();
                if(isClientPath(path)){
                    String clientName = retrieveClientName(path);
                    logger.info("Found new client : " + clientName);
                    Map<String, String> initialConfig = new HashMap<>();
                    try {
                         if(event.getData().getData()!=null) {
                             initialConfig = jsonMapper.readValue(
                                     event.getData().getData(), new TypeReference<Map<String, String>>() {
                                     });
                         }

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
                String[] clientAndNode = location.replace("/" + CLIENT_LIST_LOCATION + "/", "").split("/");
                if(clientAndNode !=null && clientAndNode.length==2){
                    for(ClientConfigUpdateListener listener: listeners){
                        foundAMatch = true;
                        byte[] data = event.getData().getData();
                        String dataString = data == null ? "" : new String(data);
                        listener.configUpdated(clientAndNode[0], clientAndNode[1],dataString);
                    }

                } else {
                    logger.warn("Couldn't process message for node : " + location + " data : " + new String(event.getData().getData()));
                }
                if (!foundAMatch)
                    logger.warn("Received message for node " + location +" : " + event.getType() + " but found no interested listeners");

                break;
            case NODE_REMOVED:
                path = event.getData().getPath();
                String[] clientAndNode2 = path.replace("/" + CLIENT_LIST_LOCATION + "/", "").split("/");
                if(clientAndNode2 !=null && clientAndNode2.length==2){
                    for(ClientConfigUpdateListener listener: listeners){
                    	listener.configRemoved(clientAndNode2[0], clientAndNode2[1]);
                    }
                }
                if(isClientPath(path)){
                    String clientName = retrieveClientName(path);
                    clientsWithInitialConfig.keySet().remove(clientName);
                    logger.warn("Deleted client : " + clientName+" - presently resources will not be released");
                    //for (NewClientListener listener: newClientListeners)
                    //    listener.clientDeleted(clientName);
                    //jdofactory.clientDeleted(clientName); // ensure called last in case other client removal listeners need db
                }
                break;
            case INITIALIZED:
                initalized = true;
                logger.info("Finished building '/all_clients' tree cache. ");
                afterCacheBuilt();

        }
    }

    public static String retrieveClientName(String path) {
        return path.replace("/"+CLIENT_LIST_LOCATION+"/","").split("/")[0];
    }

    private void afterCacheBuilt() throws Exception {
        // first get the clients
        Collection<ChildData> clientChildrenData = handler.getImmediateChildren("/" + CLIENT_LIST_LOCATION);
        logger.info("Found " +clientChildrenData.size() + " clients on start up.");
        for(ChildData clientChildData : clientChildrenData) {
            childEvent(null, new TreeCacheEvent(TreeCacheEvent.Type.NODE_ADDED, clientChildData));
            // then the children of clients
            Collection<ChildData> furtherChildren = handler.getChildren(clientChildData.getPath());
            logger.info("Found " +furtherChildren.size() + " children for client "+ retrieveClientName(clientChildData.getPath())+" on startup");
            for (ChildData child : furtherChildren){
                childEvent(null, new TreeCacheEvent(TreeCacheEvent.Type.NODE_ADDED, child));
            }
        }

    }

    public void contextIntialised() throws Exception {
        handler.addSubscription("/" + CLIENT_LIST_LOCATION, this);
    }
}
