/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.api.logging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.apache.log4j.Logger;

public class ActionLogger {
	private static Logger actionLogger = Logger.getLogger( "ActionLogger" );
	
	public static void logAsJson(String client,long userId,long itemId,Integer type,Double value,String clientUserId,String clientItemId,String recTag, String extra_data) throws IOException {
	    String actionEvent = getActionLogAsJson(client, userId, itemId, type, value, clientUserId, clientItemId, recTag, extra_data);
		actionLogger.info(actionEvent);
	}
	
    public static String getActionLogAsJson(String client,long userId,long itemId,Integer type,Double value,String clientUserId,String clientItemId,String recTag, String extra_data) throws IOException {
        
        if (type == null) { type = 1; }
        if (value == null) { value = 1D; }
        recTag = (recTag != null) ? recTag : "default";
        extra_data = (extra_data != null) ? extra_data : "{}";
        
        Object extra_data_object = null;
        { // check if extra_data is actually a json object, else use the extra_data as is
            try {
                ObjectMapper mapper = new ObjectMapper();
                HashMap<String,Object> o = mapper.readValue(extra_data, HashMap.class);
                extra_data_object = o;
            } catch (IOException e) {
                extra_data_object = extra_data;
            }
        }
        
        String json;
        { // build the json object and write as a string
            Map<String,Object> keyVals = new HashMap<String,Object>();
            
            keyVals.put("client", client);
            keyVals.put("client_itemid", clientItemId);
            keyVals.put("client_userid", clientUserId);
            keyVals.put("itemid", String.valueOf(itemId));
            keyVals.put("rectag", recTag);
            keyVals.put("type", String.valueOf(type));
            keyVals.put("userid", String.valueOf(userId));
            keyVals.put("value", String.valueOf(value));
            keyVals.put("extra_data", extra_data_object);
            
            ObjectMapper mapper = new ObjectMapper();
                
            json = mapper.writeValueAsString(keyVals);
        }

        return json;
    }
}
