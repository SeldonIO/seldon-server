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
package io.seldon.api.logging;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.seldon.api.rpc.ClassificationReply;
import io.seldon.api.rpc.ClassificationRequest;
import io.seldon.prediction.PredictionServiceResult;
import io.seldon.rpc.ClientRpcStore;


@Component
public class PredictLogger {

	private static Logger predictLogger = Logger.getLogger( "PredictLogger" );
	
	@Autowired 
	ClientRpcStore rpcStore;
	
	public void log(String client,JsonNode input,PredictionServiceResult response)
	{
		ObjectMapper mapper = new ObjectMapper();
		JsonNode prediction = mapper.valueToTree(response);
		ObjectNode topNode = mapper.createObjectNode();
		topNode.put("consumer", client);
		topNode.put("input", input);
		topNode.put("prediction", prediction);
		predictLogger.info(topNode.toString());
	}
	
	public void log(String client,ClassificationRequest request,ClassificationReply reply)
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode topNode = mapper.createObjectNode();
		topNode.put("consumer", client);
		topNode.put("input", rpcStore.getJSONForRequest(client, request));
		topNode.put("prediction", rpcStore.getJSONForReply(client, reply));
		predictLogger.info(topNode.toString());
	}
}
