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
 * Provides a list of algorithm strategies to use for a client for a given user.
 * @author firemanphil
 *         Date: 19/02/15
 *         Time: 15:04
 */
public interface ClientStrategy {

    Double getDiversityLevel(String userId, String recTag);

    List<AlgorithmStrategy> getAlgorithms(String userId, String recTag);

    AlgorithmResultsCombiner getAlgorithmResultsCombiner(String userId, String recTag);

    String getName(String userId);
    
    Map<Integer,Double> getActionsWeights(String userId, String recTag);
}
