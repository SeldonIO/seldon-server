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
package io.seldon.stream.analytics;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;


//Request(consumer: String, time : Long, httpmethod : String, path : String, exectime : Int, count : Int)
public class Request {

	Pattern recommendationPattern = Pattern.compile("(/users/)([^/]+)(/recommendations)");
	Pattern actionPattern = Pattern.compile("(/users/)([^/]+)(/actions)");
	Pattern actionPattern2 = Pattern.compile("(/items/)([^/]+)(/actions)");
	Pattern actionPattern3 = Pattern.compile("(/users/)([^/]+)(/actions/)([^/]+)");

	// /users/{userId}/actions/{itemId}
	// /items/{itemId}/actions/{userId}
	String consumer;
	Long time;
	String httpmethod;
	String path;
	Integer exectime;
	Integer count;
	
	public Request() {}
	
	public Request(JsonNode j)
	{
		consumer = j.get("consumer").asText();
		time = j.get("time").asLong();
		httpmethod = j.get("httpmethod").asText();
		
		path = createPath(j.get("path").asText());
		exectime = j.get("exectime").asInt();
		count = 1;
	}
	
	private String createPath(String path)
	{
		final Matcher recMatcher = recommendationPattern.matcher(path);
		final Matcher actionMatcher = actionPattern.matcher(path);
		final Matcher actionMatcher2 = actionPattern2.matcher(path);
		final Matcher actionMatcher3 = actionPattern3.matcher(path);
		if (recMatcher.matches()) 
        	return recMatcher.replaceFirst("$1{userid}$3");
		else if (actionMatcher3.matches()) 
			return actionMatcher3.replaceFirst("$1{userid}$3{itemid}");
		else if (actionMatcher.matches()) 
			return actionMatcher.replaceFirst("$1{userid}$3");
		else if (actionMatcher2.matches()) 
			return actionMatcher2.replaceFirst("$1{itemid}$3");
		
		else
			return path;	
	}
	
	public Request add(Request other)
	{
		count += other.count;
		exectime += other.exectime;
		return this;
	}

	@Override
	public String toString() {
		return "Request [consumer=" + consumer + ", time=" + time
				+ ", httpmethod=" + httpmethod + ", path=" + path
				+ ", exectime=" + exectime + ", count=" + count + "]";
	}
	
	
	
}
