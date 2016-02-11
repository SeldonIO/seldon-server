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
package io.seldon.spark.actions;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JobUtils {

    public static String getSourceDirFromDate(String path_pattern, String date_string) {
        String retVal = null;
        if (date_string.length() == 8) {
            String y = date_string.substring(0, 4);
            String m = date_string.substring(4, 6);
            String d = date_string.substring(6, 8);

            retVal = path_pattern;
            retVal = retVal.replace("%y", y);
            retVal = retVal.replace("%m", m);
            retVal = retVal.replace("%d", d);
        }
        return retVal;
    }

    /*
     * eg. "2014-09-19T14:20:14Z" -> 1411136414
     */
    public static long utc_to_unixts(String utc_date) throws ParseException {
        DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date date = utcFormat.parse(utc_date);
        return (date.getTime() / 1000);
    }

    public static ActionData getActionDataFromActionLogLine(ObjectMapper objectMapper, String actionLogLine) {
    	ActionData actionData = null;
    	
        String[] parts = actionLogLine.split("\\s+", 3);
        String json = parts[2];
    	
    	try {
			actionData = objectMapper.readValue(json, ActionData.class);
		} catch (IOException e) {
			e.printStackTrace();
		}

    	if (actionData != null) {
            actionData.timestamp_utc = parts[0];    	
    	}
    	return actionData;
    }
    
    public static String getJsonFromActionData(ActionData actionData) {
        JsonFactory jsonFactory = new JsonFactory();
        StringWriter sw = new StringWriter();
        try {
            JsonGenerator jg = jsonFactory.createGenerator(sw);
            jg.writeStartObject();
            jg.writeStringField("timestamp_utc", actionData.timestamp_utc);
            jg.writeStringField("client", actionData.client);
            jg.writeStringField("client_userid", actionData.client_userid);
            jg.writeNumberField("userid", actionData.userid);
            jg.writeNumberField("itemid", actionData.itemid);
            jg.writeStringField("client_itemid", actionData.client_itemid);
            jg.writeStringField("rectag", actionData.rectag);
            jg.writeNumberField("type", actionData.type);
            jg.writeNumberField("value", actionData.value);
            if (actionData.extra_data != null) {
            	jg.writeFieldName("extra_data");
            	if (actionData.extra_data instanceof LinkedHashMap) {
                	jg.writeStartObject();
            		Map<String, String> extra_data_map = (LinkedHashMap<String,String>)actionData.extra_data;
            		for (Map.Entry<String, String> entry: extra_data_map.entrySet()) {
            			jg.writeStringField(entry.getKey(), entry.getValue());
            		}
                	jg.writeEndObject();
            	} else if (actionData.extra_data instanceof String) {
        			jg.writeString((String)actionData.extra_data);
            	} else {
        			jg.writeNull();
            	}
            }
            jg.writeEndObject();
            jg.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sw.toString();
    }

    public static String generateManifestFromClientMap(Map<String, Integer> clientPartitionMap) {
        StringBuilder retVal = new StringBuilder();

        for (Map.Entry<String, Integer> entry : clientPartitionMap.entrySet()) {
            String client = entry.getKey();
            String partition_index = entry.getValue().toString();
            Map<String, String> m = new HashMap<String, String>();
            m.put("client", client);
            m.put("partition_index", partition_index);
            String json = mapToJson(m);
            retVal.append(json);
            retVal.append("\n");
        }
        return retVal.toString();
    }

    public static String mapToJson(Map<String, String> m) {
        JsonFactory jsonFactory = new JsonFactory();
        StringWriter sw = new StringWriter();
        try {
            JsonGenerator jg = jsonFactory.createGenerator(sw);
            jg.writeStartObject();
            for (Map.Entry<String, String> entry : m.entrySet()) {
                jg.writeStringField(entry.getKey(), entry.getValue());
            }
            jg.writeEndObject();
            jg.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sw.toString();
    }

    public static long dateToUnixDays(String dateString) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = dateFormat.parse(dateString);

        return ((date.getTime() / 1000) / 86400);
    }
}
