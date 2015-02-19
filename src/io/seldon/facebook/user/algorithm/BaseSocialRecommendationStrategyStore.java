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

package io.seldon.facebook.user.algorithm;

import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTest;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author philipince
 *         Date: 07/11/2013
 *         Time: 18:39
 */
@Component
public class BaseSocialRecommendationStrategyStore implements SocialRecommendationStrategyStore {


    @Resource(name = "clientToStrategy")
    private Map<String, SocialRecommendationStrategy> clientToAlgorithmStore;

    private ConcurrentMap<String, MultiVariateTest<SocialRecommendationStrategy>> clientToTest
            = new ConcurrentHashMap<>();

    @Override
    public SocialRecommendationStrategy getStrategy(String userId, String client) {
        MultiVariateTest<SocialRecommendationStrategy> test = clientToTest.get(client);
        if(test!=null){
            return test.sample(userId);
        } else {
            return clientToAlgorithmStore.get(client);
        }
    }

    @Override
    public Set<String> getAvailableClients() {
        return clientToAlgorithmStore.keySet();
    }

    public ConcurrentMap<String, MultiVariateTest<SocialRecommendationStrategy>> getClientToTest() {
        return clientToTest;
    }

    public Map<String, SocialRecommendationStrategy> getClientToAlgorithmStore() {
        return clientToAlgorithmStore;
    }
}
