package io.seldon.rpc;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import io.grpc.ManagedChannel;
import io.seldon.api.rpc.ClassifierGrpc;
import io.seldon.api.rpc.ClassifierGrpc.ClassifierBlockingStub;
import io.seldon.api.rpc.PredictReply;
import io.seldon.api.rpc.PredictRequest;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;
import io.seldon.prediction.PredictionAlgorithm;
import io.seldon.prediction.PredictionServiceResult;
import io.seldon.prediction.PredictionsResult;

@Component
public class RPCPredictionServer implements PredictionAlgorithm {
	private static Logger logger = Logger.getLogger(RPCPredictionServer.class.getName());
	private static final String HOST_PROPERTY_NAME="io.seldon.rpc.microservice.host";
	private static final String PORT_PROPERTY_NAME="io.seldon.rpc.microservice.port";

	final ClientRPCStore rpcStore;
	RPCChannelHandler channelHandler;
	
	@Autowired
	public RPCPredictionServer(ClientRPCStore rpcStore,RPCChannelHandler channelHandler){
        this.rpcStore = rpcStore;
        this.channelHandler = channelHandler;
    }
	
	@Override
	public PredictionServiceResult predictFromJSON(String client, JsonNode json, OptionsHolder options) {
		try
		{
			PredictRequest request = rpcStore.getPredictRequestFromJson(client, json);
			PredictReply reply = predictFromProto(client, request, options);
			JsonNode actualObj = rpcStore.getJSONForReply(client, reply);
			PredictionsResult res = null;
			JsonNode extraData = null;
			ObjectMapper mapper = new ObjectMapper();
			if (actualObj.has("prediction"))
			{
				ObjectReader reader = mapper.reader(PredictionsResult.class);
				String predictionStr = actualObj.get("prediction").toString();
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
		finally{}
	}

	@Override
	public PredictReply predictFromProto(String client, PredictRequest request, OptionsHolder options) {
			ManagedChannel channel = channelHandler.getChannel(options.getStringOption(HOST_PROPERTY_NAME), options.getIntegerOption(PORT_PROPERTY_NAME));
			ClassifierBlockingStub stub =  ClassifierGrpc.newBlockingStub(channel);
			PredictReply reply =  stub.predict(request);
			return reply;
	}

}
