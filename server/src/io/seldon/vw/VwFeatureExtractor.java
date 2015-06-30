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
package io.seldon.vw;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.springframework.stereotype.Component;

@Component
public class VwFeatureExtractor {

	
	public List<Namespace> extract(JsonNode json)
	{
		List<Namespace> namespaces = new ArrayList<Namespace>();
		
		Map<String,Float> features = new HashMap<String,Float>();
		for(Iterator<String> it = json.getFieldNames();it.hasNext();)
		{
			String field = it.next();
			JsonNode jnode = json.get(field);
			if (jnode.isNumber())
			{
				features.put(field, (float)jnode.asDouble());
			}
			else if (jnode.isObject())
			{
				namespaces.add(extractNamespace(field, jnode));
			}
		}
		namespaces.add(new Namespace(features));
		
		return namespaces;
	}
	
	private Namespace extractNamespace(String name,JsonNode json)
	{
		Map<String,Float> features = new HashMap<String,Float>();
		for(Iterator<String> it = json.getFieldNames();it.hasNext();)
		{
			String field = it.next();
			JsonNode jnode = json.get(field);
			if (jnode.isNumber())
			{
				features.put(field, (float)jnode.asDouble());
			}
		}
		return new Namespace(name,features);
	}
	
	
	public static class Namespace
	{
		final String name;
		final Map<String,Float> features;
		
		public Namespace(Map<String, Float> features) {
			super();
			this.features = features;
			this.name = "";
		}

		public Namespace(String name, Map<String, Float> features) {
			super();
			this.name = name;
			this.features = features;
		}

		@Override
		public String toString() {
			return "Namespace [name=" + name + ", features=" + features + "]";
		}

	}
	
	
	
}
