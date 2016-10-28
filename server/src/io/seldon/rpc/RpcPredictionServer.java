package io.seldon.rpc;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import io.grpc.ManagedChannel;
import io.seldon.api.rpc.ClassificationReply;
import io.seldon.api.rpc.ClassificationRequest;
import io.seldon.api.rpc.ClassifierGrpc;
import io.seldon.api.rpc.ClassifierGrpc.ClassifierBlockingStub;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;
import io.seldon.prediction.PredictionAlgorithm;
import io.seldon.prediction.PredictionServiceResult;
import io.seldon.prediction.PredictionsResult;

@Component
public class RpcPredictionServer implements PredictionAlgorithm {
	private static Logger logger = Logger.getLogger(RpcPredictionServer.class.getName());
	private static final String name = RpcPredictionServer.class.getName();
	private static final String HOST_PROPERTY_NAME="io.seldon.rpc.microservice.host";
	private static final String PORT_PROPERTY_NAME="io.seldon.rpc.microservice.port";

	final ClientRpcStore rpcStore;
	RpcChannelHandler channelHandler;
	
	@Autowired
	public RpcPredictionServer(ClientRpcStore rpcStore,RpcChannelHandler channelHandler){
        this.rpcStore = rpcStore;
        this.channelHandler = channelHandler;
    }
	
	 public String getName()
	 {
		 return name;
	 }
	
	@Override
	public PredictionServiceResult predictFromJSON(String client, JsonNode json, OptionsHolder options) {
		try
		{
			ClassificationRequest request = rpcStore.getPredictRequestFromJson(client, json);
			ClassificationReply reply = predictFromProto(client, request, options);
			JsonNode actualObj = rpcStore.getJSONForReply(client, reply);
			PredictionsResult res = null;
			JsonNode extraData = null;
			ObjectMapper mapper = new ObjectMapper();
			if (actualObj.has("predictions"))
			{
				ObjectReader reader = mapper.reader(PredictionsResult.class);
				String predictionStr = actualObj.get("predictions").toString();
				res = reader.readValue(predictionStr);
			}
			if (actualObj.has("custom"))
			{
				extraData = actualObj.get("custom");
			}
			return new PredictionServiceResult(res, extraData);
		} catch (JsonProcessingException e) {
			logger.error("Couldn't retrieve prediction from external prediction server - ", e);
			return null;
		} catch (IOException e) {
			logger.error("Couldn't retrieve prediction from external prediction server - ", e);
			return null;
		}
		catch (Exception e)
		{
			logger.error("Couldn't retrieve prediction from external prediction server - ", e);
			return null;
		}
		finally{}
	}

	@Override
	public ClassificationReply predictFromProto(String client, ClassificationRequest request, OptionsHolder options) {
			ManagedChannel channel = channelHandler.getChannel(options.getStringOption(HOST_PROPERTY_NAME), options.getIntegerOption(PORT_PROPERTY_NAME));
			ClassifierBlockingStub stub =  ClassifierGrpc.newBlockingStub(channel);
			ClassificationReply reply =  stub.predict(request);
			return reply;
	}

}
