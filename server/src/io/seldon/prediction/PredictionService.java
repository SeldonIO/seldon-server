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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.seldon.api.APIException;
import io.seldon.api.logging.PredictLogger;
import io.seldon.api.resource.PredictionBean;
import io.seldon.api.state.PredictionAlgorithmStore;
import io.seldon.api.state.options.DefaultOptions;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;
import io.seldon.memcache.SecurityHashPeer;

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

	
	public JsonNode predict(String client,String puid, JsonNode json)
	{
		PredictionStrategy strategyTop = algStore.retrieveStrategy(client);
		if (strategyTop == null) {
	            throw new APIException(APIException.NOT_VALID_STRATEGY);
		}
		
		SimplePredictionStrategy strategy = strategyTop.configure();
		
		// transform features
		for(FeatureTransformerStrategy transStr : strategy.getFeatureTansformers())
		{
			json = transStr.transformer.transform(client, json, transStr);
		}
		
		if (puid == null)
			puid = SecurityHashPeer.getNewId();
	
		// apply prediction algorithm(s)
		for(PredictionAlgorithmStrategy algStr : strategy.getAlgorithms())
		{
			OptionsHolder optsHolder = new OptionsHolder(defaultOptions, algStr.config);
			PredictionServiceResult predictionServiceResult = algStr.algorithm.predictFromJSON(client, json, optsHolder);
			PredictionsResult res = predictionServiceResult.predictions;
			//FIXME enforces first successful combiner at present			
			if (res != null && res.predictions.size() > 0)
			{
				PredictLogger.log(client,algStr.name, json, res,strategy.label,puid);
				List<PredictionBean> pbeans = new ArrayList<>();
				for(PredictionResult r : res.predictions)
				{
					pbeans.add(new PredictionBean(r.prediction, r.predictedClass,r.confidence));
				}
				
				//TODO clean this up
				ObjectMapper mapper = new ObjectMapper();
				JsonNode prediction = mapper.valueToTree(res);
				ObjectNode topNode = mapper.createObjectNode();
				topNode.put("prediction",prediction);
				if (predictionServiceResult.extra != null)
					topNode.put("extra", predictionServiceResult.extra);
				PredictionMetadata meta = new PredictionMetadata(res.model, strategy.label, puid);
				JsonNode metaJson = mapper.valueToTree(meta);
				topNode.put("meta", metaJson);
				logger.error(topNode.toString());
				return topNode;
			}
		}
		
		logger.warn("No prediction for client "+client+" with json "+json);
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode topNode = mapper.createObjectNode();
		PredictionMetadata meta = new PredictionMetadata("", strategy.label, puid);
		return topNode;
	}
	
}
