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

package io.seldon.facebook.user.algorithm.experiment;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.seldon.general.MgmAction;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author philipince
 *         Date: 08/10/2013
 *         Time: 17:59
 */
@Component
public class MultiVariateTestStore {

    private static final Logger logger = Logger.getLogger(MultiVariateTestStore.class);
    private ConcurrentMap<String,MultiVariateTest> clientKeyToTest;

    public MultiVariateTestStore(){
        this.clientKeyToTest = new ConcurrentHashMap<>();
    }
    public MultiVariateTestStore(ConcurrentMap<String,MultiVariateTest> clientKeyToTest){
        this.clientKeyToTest = clientKeyToTest;
    }

    public void addTest(String client, MultiVariateTest test){
        clientKeyToTest.put(client, test);
    }

    public boolean testRunning(String client){
        return clientKeyToTest.containsKey(client);
    }

    public void registerTestEvent(String clientKey, MgmAction action){
        MultiVariateTest test = clientKeyToTest.get(clientKey);
        String variationKey = retrieveVariationKey(clientKey, action.getPrimaryUserId(), test);
        clientKeyToTest.get(clientKey).registerTestEvent(variationKey, action);
    }

    public static String retrieveVariationKey(String clientKey, String userId, MultiVariateTest test){
        String multiVariateTestKey = test.sampleVariations(userId);
        logger.info("Generated testing key of " + multiVariateTestKey + " for userid "+ userId);

        return multiVariateTestKey;
    }

    public String retrieveVariationKey(String clientKey, String userId){
        MultiVariateTest test = clientKeyToTest.get(clientKey);
        return retrieveVariationKey(clientKey, userId, test);
    }

    public Multimap<String, MultiVariateTestResult> getResults(){
        Multimap<String, MultiVariateTestResult> results = HashMultimap.create();
        for(Map.Entry<String,MultiVariateTest> entry : clientKeyToTest.entrySet()){
            results.putAll(entry.getKey(), getResults(entry.getKey()));
        }
        return results;
    }

    public List<MultiVariateTestResult> getResults(String clientKey){
        if(testRunning(clientKey)){
            return clientKeyToTest.get(clientKey).retrieveResults();
        }
        return Collections.emptyList();
    }

    public void removeTest(String client) {
        clientKeyToTest.remove(client);
    }
}
