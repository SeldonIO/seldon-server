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

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.seldon.api.APIException;
import io.seldon.api.logging.PredictLogger;
import io.seldon.api.rpc.ClassificationReply;
import io.seldon.api.rpc.ClassificationReplyMeta;
import io.seldon.api.rpc.ClassificationRequest;
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
	
	public ClassificationReply predict(String client,ClassificationRequest request)
	{
		PredictionStrategy strategyTop = algStore.retrieveStrategy(client);
		if (strategyTop == null) {
	            throw new APIException(APIException.NOT_VALID_STRATEGY);
		}
		
		SimplePredictionStrategy strategy = strategyTop.configure();
		
		// apply prediction algorithm(s)
		for(PredictionAlgorithmStrategy algStr : strategy.getAlgorithms())
		{
			OptionsHolder optsHolder = new OptionsHolder(defaultOptions, algStr.config);
			ClassificationReply res = algStr.algorithm.predictFromProto(client, request, optsHolder);
			if (res != null && res.getPredictionsList() != null && res.getPredictionsCount() > 0)
			{
				if (res.getMeta() == null)
				{
					ClassificationReplyMeta meta = ClassificationReplyMeta.newBuilder().setVariation(strategy.label).setPuid(SecurityHashPeer.getNewId()).setModelName(algStr.name).build();
					return ClassificationReply.newBuilder().setCustom(res.getCustom()).setMeta(meta).addAllPredictions(res.getPredictionsList()).build();
				}
				else
				{
					ClassificationReplyMeta.Builder metaBuilder = ClassificationReplyMeta.newBuilder();
					if (!request.hasMeta() || StringUtils.isEmpty(request.getMeta().getPuid()))
						metaBuilder.setPuid(SecurityHashPeer.getNewId());
					else
						metaBuilder.setPuid(res.getMeta().getPuid());
					ClassificationReplyMeta meta = 	metaBuilder.setVariation(strategy.label).setModelName(res.getMeta().getModelName()).build();
					return ClassificationReply.newBuilder().setCustom(res.getCustom()).setMeta(meta).addAllPredictions(res.getPredictionsList()).build();
				}
			}
		}
		
		logger.warn("No prediction for client "+client);
		return ClassificationReply.newBuilder().build();
	}

	
	public PredictionServiceResult predict(String client,String puid, JsonNode json)
	{
		PredictionStrategy strategyTop = algStore.retrieveStrategy(client);
		if (strategyTop == null) {
	            throw new APIException(APIException.NOT_VALID_STRATEGY);
		}
		
		SimplePredictionStrategy strategy = strategyTop.configure();
		
		// transform features
		//for(FeatureTransformerStrategy transStr : strategy.getFeatureTansformers())
		//{
		//	json = transStr.transformer.transform(client, json, transStr);
		//}
		
		if (puid == null)
			puid = SecurityHashPeer.getNewId();
	
		// apply prediction algorithm(s)
		for(PredictionAlgorithmStrategy algStr : strategy.getAlgorithms())
		{
			OptionsHolder optsHolder = new OptionsHolder(defaultOptions, algStr.config);
			PredictionServiceResult predictionServiceResult = algStr.algorithm.predictFromJSON(client, json, optsHolder);
			if (predictionServiceResult != null && predictionServiceResult.predictions != null && predictionServiceResult.predictions.size() > 0)
			{
				if (predictionServiceResult.meta == null)
				{
					PredictionMetadata meta = new PredictionMetadata(algStr.name, strategy.label, puid);
					predictionServiceResult.meta = meta;
				}
				else
				{
					predictionServiceResult.meta.setPuid(puid);
					predictionServiceResult.meta.variation = strategy.label;
				}
				return predictionServiceResult;
			}
		}
		
		logger.warn("No prediction for client "+client+" with json "+json);
		PredictionMetadata meta = new PredictionMetadata("", strategy.label, puid);
		PredictionServiceResult res =  new PredictionServiceResult(meta,new ArrayList<PredictionResult>(),null);
		return res;
	}
	
}
