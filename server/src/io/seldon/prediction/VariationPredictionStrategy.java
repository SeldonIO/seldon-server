/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.prediction;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.NumberRange;
import org.apache.commons.lang.math.Range;

import scala.concurrent.forkjoin.ThreadLocalRandom;

public class VariationPredictionStrategy implements PredictionStrategy{

	private final Map<Range, SimplePredictionStrategy> strategyMap;
	
	public VariationPredictionStrategy(Map<Range, SimplePredictionStrategy> strategyMap) {
		this.strategyMap = strategyMap;
	}

	@Override
	public List<PredictionAlgorithmStrategy> getAlgorithms() {
		return sample().getAlgorithms();
	}

	@Override
	public List<FeatureTransformerStrategy> getFeatureTansformers() {
		return sample().getFeatureTansformers();
	}
	
	 public SimplePredictionStrategy sample() {
	        Integer hash = ThreadLocalRandom.current().nextInt();
	        int sample = Math.abs(hash % 100) + 1;
	        BigDecimal sampleDec = BigDecimal.valueOf(sample).divide(BigDecimal.valueOf(100));
	        for (Range range : strategyMap.keySet()) {
	            if (range.containsNumber(sampleDec)) {
	                return strategyMap.get(range);
	            }
	        }
	        return null;
	    }

	@Override
	public SimplePredictionStrategy configure() {
		return sample();
	}
	
	 public static VariationPredictionStrategy build(List<Variation> variations){
	        Map<Range, SimplePredictionStrategy> strategyMap = new LinkedHashMap<>();
	        BigDecimal ratioTotal = BigDecimal.ZERO;
	        for (Variation var : variations){
	            ratioTotal = ratioTotal.add(var.ratio);
	        }
	        BigDecimal currentMax = BigDecimal.ZERO;
	        for(Variation var : variations){
	        	NumberRange range = new NumberRange(currentMax, currentMax.add(var.ratio.divide(ratioTotal, 5, BigDecimal.ROUND_UP)));
	            strategyMap.put(range,var.variationStrategy);
	            currentMax = currentMax.add(var.ratio);
	        }
	        return new VariationPredictionStrategy(strategyMap);
	    }


	    public static class Variation{
	        public final SimplePredictionStrategy variationStrategy;
	        public final BigDecimal ratio;

	        public Variation(SimplePredictionStrategy variationStrategy, BigDecimal ratio) {
	            this.variationStrategy = variationStrategy;
	            this.ratio = ratio;
	        }
	    }

}
