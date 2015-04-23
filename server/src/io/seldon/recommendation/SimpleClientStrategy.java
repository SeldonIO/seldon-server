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
 * A client strategy that provides the same algorithm strategies for every user.
 *
 * @author firemanphil
 *         Date: 01/12/14
 *         Time: 14:35
 */
public class SimpleClientStrategy implements ClientStrategy {

    private final List<AlgorithmStrategy> strategies;

    private final AlgorithmResultsCombiner algResultsCombiner;
    private final Double diversityLevel;
    private final String name;
    private final Map<Integer,Double> actionWeights;

    public SimpleClientStrategy(List<AlgorithmStrategy> strategies, AlgorithmResultsCombiner algResultsCombiner,
                                Double diversityLevel, String name,Map<Integer,Double> actionWeights) {
        this.strategies = strategies;
        this.algResultsCombiner = algResultsCombiner;
        this.diversityLevel= diversityLevel==null? 1.0 : diversityLevel;
        this.name = name;
        this.actionWeights = actionWeights;
    }

    @Override
    public List<AlgorithmStrategy> getAlgorithms(String userId, String recTag) {
        return strategies;
    }

    @Override
    public AlgorithmResultsCombiner getAlgorithmResultsCombiner(String userId, String recTag) {
        return algResultsCombiner;
    }

    @Override
    public String getName(String userId) {
        return name;
    }

    @Override
    public Double getDiversityLevel(String userId, String recTag) {
        return diversityLevel;
    }

	@Override
	public Map<Integer, Double> getActionsWeights(String userId, String recTag) {
		return actionWeights;
	}
}
