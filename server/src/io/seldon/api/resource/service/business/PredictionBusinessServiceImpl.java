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
package io.seldon.api.resource.service.business;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.logging.EventLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.EventBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.PredictionBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.external.ExternalPredictionServer;
import io.seldon.prediction.PredictionResult;
import io.seldon.prediction.PredictionsResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PredictionBusinessServiceImpl implements PredictionBusinessService {

	private static final String JSON_KEY = "json";
	private static final String CLIENT_KEY = "client";
	private static final String TIMESTAMP_KEY = "timestamp";
	
	@Autowired
	ExternalPredictionServer predictionServer;
	
	private boolean allowedKey(String key)
	{
		return (!(Constants.CONSUMER_KEY.equals(key) || 
					Constants.CONSUMER_SECRET.equals(key) ||
					Constants.OAUTH_TOKEN.equals(key) ||
					CLIENT_KEY.equals(key) ||
					"jsonpCallback".equals(key)));
		
	}
	
	private Long getTimeStamp()
	{
		return System.currentTimeMillis()/1000;
	}
	
	private String getValidatedJson(ConsumerBean consumer,String jsonRaw) throws JsonParseException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getJsonFactory();
	    JsonParser parser = factory.createJsonParser(jsonRaw);
	    JsonNode actualObj = mapper.readTree(parser);
	    ((ObjectNode) actualObj).put(CLIENT_KEY,consumer.getShort_name());
	    if (actualObj.get(TIMESTAMP_KEY) == null)
	    {
	    	((ObjectNode) actualObj).put(TIMESTAMP_KEY,getTimeStamp());
	    }
	    else
	    {
	    	JsonNode timeNode = actualObj.get(TIMESTAMP_KEY);
	    	if (!(timeNode.isInt() || timeNode.isLong()))
	    	{
	    		throw new APIException(APIException.INVALID_JSON);
			}
		}
	    return actualObj.toString();
	}
	
	private ResourceBean getValidatedJsonResource(ConsumerBean consumer,String jsonRaw)
	{
		ResourceBean responseBean;
		try
		{
			String json = getValidatedJson(consumer, jsonRaw);
			EventLogger.log(json);
			responseBean = new EventBean(json);
	    } 
		catch (IOException e) 
		{
			ApiLoggerServer.log(this, e);
			APIException apiEx = new APIException(APIException.INVALID_JSON);
			responseBean = new ErrorBean(apiEx);
		}
		catch (APIException e)
		{
			ApiLoggerServer.log(this, e);
			responseBean = new ErrorBean(e);
		}
	    return responseBean;
	}
	
	@Override
	public ResourceBean addEvent(ConsumerBean consumerBean,Map<String, String[]> parameters) {
		ResourceBean responseBean;
		Map<String,Object> keyVals = new HashMap<String,Object>();
		if (parameters.containsKey(JSON_KEY))
		{
			String jsonRaw = parameters.get(JSON_KEY)[0];
			responseBean = getValidatedJsonResource(consumerBean,jsonRaw);
		}
		else
		{
			for(Map.Entry<String, String[]> reqMapEntry : parameters.entrySet())
			{
				if (reqMapEntry.getValue().length == 1 && allowedKey(reqMapEntry.getKey()))
				{
					keyVals.put(reqMapEntry.getKey(), reqMapEntry.getValue()[0]);
				}
			}
			keyVals.put(CLIENT_KEY, consumerBean.getShort_name());
			if (!keyVals.containsKey(TIMESTAMP_KEY))
				keyVals.put(TIMESTAMP_KEY, getTimeStamp());
			else
			{
				try
				{
					Long.parseLong(((String)keyVals.get(TIMESTAMP_KEY)));
				}
				catch (NumberFormatException e)
				{
					ApiLoggerServer.log(this, e);
					APIException apiEx = new APIException(APIException.INVALID_JSON);
					responseBean = new ErrorBean(apiEx);
					return responseBean;
				}
			}
			ObjectMapper mapper = new ObjectMapper();
			try {
				String json = mapper.writeValueAsString(keyVals);
				EventLogger.log(json);
				responseBean = new EventBean(json);
			} catch (IOException e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.INVALID_JSON);
				responseBean = new ErrorBean(apiEx);
			}
		}
		return responseBean;
	}

	@Override
	public ResourceBean addEvent(ConsumerBean consumerBean, String event) {
		return getValidatedJsonResource(consumerBean,event);
	}

	@Override
	public ResourceBean predict(ConsumerBean consumer, String jsonRaw) {
		ResourceBean responseBean;
		try
		{
			String json = getValidatedJson(consumer, jsonRaw);
			PredictionsResult res = predictionServer.predict(consumer.getShort_name(), json);
			ListBean listBean = new ListBean();
			for(PredictionResult r : res.predictions)
			{
				listBean.addBean(new PredictionBean(r.score, r.predictedClass,r.confidence));
			}
			listBean.setSize(res.predictions.size());
			responseBean = listBean;
	    } 
		catch (IOException e) 
		{
			ApiLoggerServer.log(this, e);
			APIException apiEx = new APIException(APIException.INVALID_JSON);
			responseBean = new ErrorBean(apiEx);
		}
		catch (APIException e)
		{
			ApiLoggerServer.log(this, e);
			responseBean = new ErrorBean(e);
		}
	    return responseBean;
	}

}
