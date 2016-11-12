package io.seldon.rpc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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
import io.seldon.api.rpc.SeldonGrpc;
import io.seldon.api.rpc.SeldonGrpc.SeldonBlockingStub;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;
import io.seldon.prediction.PredictionAlgorithm;
import io.seldon.prediction.PredictionServiceResult;

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
			PredictionServiceResult res = null;
			ObjectMapper mapper = new ObjectMapper();
			ObjectReader reader = mapper.reader(PredictionServiceResult.class);
			res = reader.readValue(actualObj);
			return res;
			
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
			ManagedChannel channel = channelHandler.getChannel(client,options.getStringOption(HOST_PROPERTY_NAME), options.getIntegerOption(PORT_PROPERTY_NAME));
			SeldonBlockingStub stub =  SeldonGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
			long t1 = System.currentTimeMillis();
			logger.info("call start");
			ClassificationReply reply =  stub.classify(request);
			long t2 = System.currentTimeMillis();
			long duration = t2-t1;
			logger.info("call end "+duration);
			return reply;
	}

}
