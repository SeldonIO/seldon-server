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

import io.seldon.api.APIException;
import io.seldon.api.state.PredictionAlgorithmStore;
import io.seldon.api.state.options.DefaultOptions;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PredictionService {
	private static Logger logger = Logger.getLogger(PredictionService.class.getName());
	private final DefaultOptions defaultOptions;
	private PredictionAlgorithmStore algStore;

	@Autowired
    public PredictionService(PredictionAlgorithmStore algStore, DefaultOptions defaultOptions) {
        this.algStore = algStore;
		this.defaultOptions = defaultOptions;
    }

	
	public PredictionsResult predict(String client,String json)
	{
		PredictionStrategy strategy = algStore.retrieveStrategy(client);
		if (strategy == null) {
	            throw new APIException(APIException.NOT_VALID_STRATEGY);
		}
		for(PredictionAlgorithmStrategy algStr : strategy.getAlgorithms())
		{
			OptionsHolder optsHolder = new OptionsHolder(defaultOptions, algStr.config);
			PredictionsResult res = algStr.algorithm.predict(client, json, optsHolder);
			if (res != null && res.predictions.size() > 0)
				return res;
		}
		
		logger.warn("No prediction for client "+client+" with json "+json);
		return new PredictionsResult(new ArrayList<PredictionResult>());
	}
	
}
