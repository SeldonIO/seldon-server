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

import java.util.List;

public class SimplePredictionStrategy implements PredictionStrategy {

	public final String label;
	public final List<FeatureTransformerStrategy> featureTransformerStrategies;
	public final List<PredictionAlgorithmStrategy> algorithmsStrategies;
	
	
	
	



	public SimplePredictionStrategy(String label,
			List<FeatureTransformerStrategy> featureTransformerStrategies,
			List<PredictionAlgorithmStrategy> algorithmsStrategies) {
		super();
		this.label = label;
		this.featureTransformerStrategies = featureTransformerStrategies;
		this.algorithmsStrategies = algorithmsStrategies;
	}


	@Override
	public List<PredictionAlgorithmStrategy> getAlgorithms() {
		return algorithmsStrategies;
	}


	@Override
	public List<FeatureTransformerStrategy> getFeatureTansformers() {
		return featureTransformerStrategies;
	}


	@Override
	public SimplePredictionStrategy configure() {
		return this;
	}

}
