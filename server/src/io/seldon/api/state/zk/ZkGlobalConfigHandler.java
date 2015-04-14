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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.seldon.api.state.GlobalConfigHandler;
import io.seldon.api.state.GlobalConfigUpdateListener;
import io.seldon.api.state.ZkNodeChangeListener;
import io.seldon.api.state.ZkSubscriptionHandler;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Zookeeper subscription service for GlobalConfigUpdateListeners
 *
 * @author firemanphil
 *         Date: 26/11/14
 *         Time: 11:11
 */
@Component
public class ZkGlobalConfigHandler implements ZkNodeChangeListener, GlobalConfigHandler, ServletContextListener {

    private static Logger logger = Logger.getLogger(ZkGlobalConfigHandler.class.getName());
    private final Multimap<String,GlobalConfigUpdateListener> nodeListeners;
    private final ZkSubscriptionHandler subHandler;
    private final static String PREFIX = "/config/";
    @Autowired
    public ZkGlobalConfigHandler( ZkSubscriptionHandler subHandler) {
        this.nodeListeners = HashMultimap.create();
        this.subHandler = subHandler;
    }

    @Override
    public void addSubscriber(String node, GlobalConfigUpdateListener listener){
        if(!nodeListeners.containsKey(node)){
            nodeListeners.put(node, listener);
            subHandler.addSubscription(PREFIX+node, this);
        } else {
            // already in the list -- just get current value
            listener.configUpdated(node, subHandler.getValue(PREFIX + node));
        }
    }

    @Override
    public void nodeChanged(String node, String value) {
        String genericNodeName = StringUtils.replace(node, PREFIX, "");
        if(nodeListeners.get(genericNodeName).isEmpty()){
            logger.warn("Couldn't find listener to tell about zk node change: " + node + " -> " + value);
        }
        for (GlobalConfigUpdateListener list : nodeListeners.get(genericNodeName)){
            list.configUpdated(genericNodeName, value);
        }
    }

    @Override
    public void nodeDeleted(String node) {
        // can't think of a reason to implement this
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        logger.info("Context init");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {

    }
}
