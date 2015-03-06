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

package io.seldon.recommendation;

import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;

import java.util.List;
import java.util.Map;

/**
 * Client strategy when using rec tags.
 * @author firemanphil
 *         Date: 04/03/15
 *         Time: 11:10
 */
public class RecTagClientStrategy implements ClientStrategy {

    private static final String DEFAULT_REC_TAG = "";
    private final ClientStrategy defaulStrategy;
    private final Map<String, ClientStrategy> recTagToStrategy;

    public RecTagClientStrategy(ClientStrategy defaulStrategy, Map<String, ClientStrategy> recTagToStrategy) {
        this.defaulStrategy = defaulStrategy;
        this.recTagToStrategy = recTagToStrategy;
    }

    @Override
    public Double getDiversityLevel(String userId, String recTag) {
        return getStrategy(recTag).getDiversityLevel(userId, recTag);
    }

    @Override
    public List<AlgorithmStrategy> getAlgorithms(String userId, String recTag) {
        return getStrategy(recTag).getAlgorithms(userId, recTag);
    }

    @Override
    public AlgorithmResultsCombiner getAlgorithmResultsCombiner(String userId, String recTag) {
        return getStrategy(recTag).getAlgorithmResultsCombiner(userId, recTag);
    }

    @Override
    public String getName() {
        return "-";
    }

    private ClientStrategy getStrategy(String recTag){
        if(recTag!=null && !recTag.equals(DEFAULT_REC_TAG)){
            // we have a rectag...
            ClientStrategy recTagStrat = recTagToStrategy.get(recTag);
            if(recTagStrat!=null){
                return recTagStrat;
            }
        }
        return defaulStrategy;
    }
}
