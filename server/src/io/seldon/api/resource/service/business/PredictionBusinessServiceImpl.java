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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.logging.EventLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.EventBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.prediction.PredictionService;

@Component
public class PredictionBusinessServiceImpl implements PredictionBusinessService {

	private static Logger logger = Logger.getLogger(PredictionBusinessServiceImpl.class.getName());
	private static final String JSON_KEY = "json";
	private static final String CLIENT_KEY = "client";
	private static final String TIMESTAMP_KEY = "timestamp";
	public static final String PUID_KEY = "puid";
	
	@Autowired
	PredictionService predictionService;
	
	private boolean allowedKey(String key)
	{
		return (!(Constants.CONSUMER_KEY.equals(key) || 
					Constants.CONSUMER_SECRET.equals(key) ||
					Constants.OAUTH_TOKEN.equals(key) ||
					CLIENT_KEY.equals(key) ||
					PUID_KEY.equals(key) || 
					"jsonpCallback".equals(key)));
		
	}
	
	private Long getTimeStamp()
	{
		return System.currentTimeMillis()/1000;
	}
	
	private JsonNode getValidatedJson(ConsumerBean consumer,String jsonRaw,boolean addExtraFeatures) throws JsonParseException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(jsonRaw);
	    JsonNode actualObj = mapper.readTree(parser);
	    if (addExtraFeatures)
	    {
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
	    }
	    return actualObj;
	}
	
	private ResourceBean getValidatedJsonResource(ConsumerBean consumer,String jsonRaw)
	{
		ResourceBean responseBean;
		try
		{
			JsonNode jsonNode = getValidatedJson(consumer, jsonRaw,true);
			String json = jsonNode.toString();
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

		if (parameters.containsKey(JSON_KEY))
		{
			String jsonRaw = parameters.get(JSON_KEY)[0];
			responseBean = getValidatedJsonResource(consumerBean,jsonRaw);
		}
		else
		{
			Map<String,Object> keyVals = new HashMap<String,Object>();
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
	public JsonNode predict(ConsumerBean consumer, String puid, String jsonRaw) {
		
		
		try
		{
			logger.info("Json raw "+jsonRaw);
			JsonNode jsonNode = getValidatedJson(consumer, jsonRaw, false); // used to check valid json but we don't use result
			return predictionService.predict(consumer.getShort_name(), puid, jsonNode);
	    } 
		catch (IOException e) 
		{
			ApiLoggerServer.log(this, e);
			APIException apiEx = new APIException(APIException.INVALID_JSON);
			ResourceBean responseBean = new ErrorBean(apiEx);
			ObjectMapper mapper = new ObjectMapper();
			JsonNode response = mapper.valueToTree(responseBean);
			return response;
		}
		catch (APIException e)
		{
			ApiLoggerServer.log(this, e);
			ResourceBean responseBean = new ErrorBean(e);
			ObjectMapper mapper = new ObjectMapper();
			JsonNode response = mapper.valueToTree(responseBean);
			return response;

		}
	}

	@Override
	public JsonNode predict(ConsumerBean consumerBean, Map<String, String[]> parameters) {
		String puid = null;
		if (parameters.containsKey(PUID_KEY))
			puid = parameters.get(PUID_KEY)[0];
		if (parameters.containsKey(JSON_KEY))
		{
			String jsonRaw = parameters.get(JSON_KEY)[0];
			return predict(consumerBean, puid, jsonRaw);
		}
		else
		{
			Map<String,Object> keyVals = new HashMap<String,Object>();
			for(Map.Entry<String, String[]> reqMapEntry : parameters.entrySet())
			{
				if (reqMapEntry.getValue().length == 1 && allowedKey(reqMapEntry.getKey()))
				{
					try 
					{
						keyVals.put(reqMapEntry.getKey(), Float.parseFloat(reqMapEntry.getValue()[0]));
					}
					catch (Exception e)
					{
						keyVals.put(reqMapEntry.getKey(), reqMapEntry.getValue()[0]);
					}
				}
			}
			ObjectMapper mapper = new ObjectMapper();
			try {
				String jsonRaw = mapper.writeValueAsString(keyVals);
				return predict(consumerBean, puid, jsonRaw);
			} catch (IOException e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.INVALID_JSON);
				ResourceBean responseBean = new ErrorBean(apiEx);
				JsonNode response = mapper.valueToTree(responseBean);
				return response;
			}
		}
	}

}
