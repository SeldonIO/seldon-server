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
import org.apache.commons.lang.math.NumberRange;
import org.apache.commons.lang.math.Range;
import org.apache.mahout.math.MurmurHash;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client strategy for testing different lists of algorthm strategies against each other. Different users get shown
 * different strategies but a given user will always be shown the same set.
 * @author firemanphil
 *         Date: 02/12/14
 *         Time: 14:16
 */
public class VariationTestingClientStrategy implements ClientStrategy {

    private final Map<Range, ClientStrategy> strategyMap;
    private static final int HASH_SEED = 5795;

    private VariationTestingClientStrategy(Map<Range, ClientStrategy> strategyMap) {
        this.strategyMap = strategyMap;
    }

    @Override
    public Double getDiversityLevel(String userId,String recTag) {
        return sample(userId).getDiversityLevel(userId, recTag);
    }

    @Override
    public List<AlgorithmStrategy> getAlgorithms(String userId, String recTag) {
        return sample(userId).getAlgorithms(userId, recTag);
    }

    @Override
    public AlgorithmResultsCombiner getAlgorithmResultsCombiner(String userId,String recTag) {
        return sample(userId).getAlgorithmResultsCombiner(userId, recTag);
    }

    @Override
    public String getName() {
        return "-";
    }

    public ClientStrategy sample(String userId) {
        Integer hash = MurmurHash.hash(userId.getBytes(), HASH_SEED);
        int sample = Math.abs(hash % 100);
        BigDecimal sampleDec = BigDecimal.valueOf(sample).divide(BigDecimal.valueOf(100));
        for (Range range : strategyMap.keySet()) {
            if (range.containsNumber(sampleDec)) {
                return strategyMap.get(range);
            }
        }
        return null;
    }

    public static VariationTestingClientStrategy build(Set<Variation> variations){
        Map<Range, ClientStrategy> strategyMap = new HashMap<>();
        BigDecimal ratioTotal = BigDecimal.ZERO;
        for (Variation var : variations){
            ratioTotal = ratioTotal.add(var.ratio);
        }
        BigDecimal currentMax = BigDecimal.ZERO;
        for(Variation var : variations){
            NumberRange range = new NumberRange(currentMax, currentMax.add(var.ratio.divide(ratioTotal)));
            strategyMap.put(range,var.variationStrategy);
            currentMax = currentMax.add(var.ratio);
        }
        return new VariationTestingClientStrategy(strategyMap);
    }


    public static class Variation{
        public final ClientStrategy variationStrategy;
        public final BigDecimal ratio;

        public Variation(ClientStrategy variationStrategy, BigDecimal ratio) {
            this.variationStrategy = variationStrategy;
            this.ratio = ratio;
        }
    }


}
