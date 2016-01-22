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

package io.seldon.recommendation.model;

import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.recommendation.ClientStrategy;
import io.seldon.resources.external.NewResourceNotifier;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

/**
 * @author firemanphil
 *         Date: 28/04/15
 *         Time: 12:16
 */
public abstract class ModelManager<T> implements PerClientExternalLocationListener {
    private static final String MODEL_PROPERTY_NAME = "io.seldon.algorithm.model.name";
    private static Logger logger = Logger.getLogger(ModelManager.class.getName());

    private final ConcurrentMap<String, ConcurrentMap<String,T>> clientStores
            = new ConcurrentHashMap<>();

    private final Executor executor;
    private final Set<String> nodeBases;

    public ModelManager(NewResourceNotifier notifier, Set<String> nodePatterns) {
        this(notifier, nodePatterns, Executors.newFixedThreadPool(2));
    }
    public ModelManager(NewResourceNotifier notifier, Set<String> nodePatterns, Executor executor) {
        this.nodeBases = nodePatterns;
        for (String pattern : nodePatterns) {
            notifier.addListener(pattern, this);
        }
        this.executor = executor;
    }

    @Override
    public void newClientLocation(final String client, final String location, final String nodePattern) {
    	logger.info("New location "+client+" : "+location+ " : "+nodePattern);
        String rightBase = null;
        Iterator<String> iter = nodeBases.iterator();
        while(rightBase==null && iter.hasNext()) {
            String base = iter.next();
            if (nodePattern.contains(base))
                rightBase = base;
        }

        final String finalPartOfNode = nodePattern.replace(rightBase,"").replaceFirst("/", "");

        executor.execute(new Runnable() {
            @Override
            public void run() {
                String key = getKey(client,nodePattern);
                logger.info("Loading with client:"+client+" location:"+location+" key:"+key+" finalPartOfNode:"+finalPartOfNode);
                T result = loadModel(location, client);
                logger.info("Loaded with client:"+client+" location:"+location+" key:"+key+"finalPartOfNodeL"+finalPartOfNode+" result:"+result);
                clientStores.putIfAbsent(key, new ConcurrentHashMap<String, T>());
                clientStores.get(key).put(finalPartOfNode, result);
                for (Map<String, T> store : clientStores.values()) {
                    for (String t : store.keySet()) {
                        logger.info(t + " " + store.get(t));
                    }
                }
            }
        });

    }

    public T getClientStore(String client, RecommendationContext.OptionsHolder options){
        String type = nodeBases.iterator().next();
        return getClientStore(client, type, options);
    }

    public T getClientStore(String client, String type, RecommendationContext.OptionsHolder options){
        String modelName = options.getStringOption(MODEL_PROPERTY_NAME);
        String key = getKey(client, type);
        if (logger.isDebugEnabled())
        	logger.debug("Get client store for client "+client+" type "+type+" modelName "+modelName+" key:"+key);
        if (!clientStores.containsKey(key))
        {
        	if (logger.isDebugEnabled())
        		logger.debug("Failed to find store with key:"+key+" for client "+client);
        	return null;
        }
        // check whether we are testing or not and get relevant model.
        switch (modelName) {
            case ClientStrategy.DEFAULT_NAME:
            	logger.debug("Returning default store for client "+modelName);
                return clientStores.get(key).get("");

            default:
                T store = clientStores.get(key).get(modelName);
                if (store == null) {
                    logger.warn("Couldn't find model under name " + modelName + " for client " + client);
                    return clientStores.get(key).get("");
                } else {
                    return store;
                }
        }

    }

    @Override
    public void clientLocationDeleted(String client, String nodePattern) {
        String rightBase = null;
        Iterator<String> iter = nodeBases.iterator();
        while(rightBase==null && iter.hasNext()) {
            String base = iter.next();
            if (nodePattern.contains(base))
                rightBase = base;
        }
        String key = getKey(client, rightBase);
        final String finalPartOfNode = nodePattern.replace(rightBase, "").replaceFirst("/", "");
        if(clientStores.get(key)!=null){
            clientStores.get(key).remove(finalPartOfNode);
        }
    }

    protected abstract T loadModel(String location,String client);

    private String getKey(String client,String key)
    {
        return client + ":" + key;
    }
}
