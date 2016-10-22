package io.seldon.rpc;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.seldon.api.rpc.PredictReply;
import io.seldon.api.rpc.PredictRequest;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;
import io.seldon.prediction.PredictionAlgorithm;
import io.seldon.prediction.PredictionServiceResult;

@Component
public class RPCPredictionServer implements PredictionAlgorithm {


	@Override
	public PredictionServiceResult predictFromJSON(String client, JsonNode json, OptionsHolder options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PredictReply predictFromProto(String client, PredictRequest request, OptionsHolder options) {
		// TODO Auto-generated method stub
		return null;
	}

}
