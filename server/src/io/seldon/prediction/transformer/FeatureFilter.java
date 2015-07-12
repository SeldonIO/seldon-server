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
package io.seldon.prediction.transformer;

import io.seldon.prediction.FeatureTransformer;
import io.seldon.prediction.FeatureTransformerStrategy;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class FeatureFilter implements FeatureTransformer {
	private static Logger logger = Logger.getLogger(FeatureFilter.class.getName());
	private static final String featureFilterType = "io.seldon.transformer.featurefilter.type";
	
	@Override
	public JsonNode transform(String client, JsonNode input,FeatureTransformerStrategy strategy) {
		
		String type = "inclusive";
		if (strategy.config.containsKey(featureFilterType))
			type = strategy.config.get(featureFilterType);
		switch(type)
		{
		case "inclusive":
		{
			Iterator<String> it = input.getFieldNames();
			for (String fieldName; it.hasNext();)
			{
				fieldName = it.next();
				if (!strategy.inputCols.contains(fieldName))
				{
					it.remove();
				}
			}
			return input;
		}
		case "exclusive":
		{
			Iterator<String> it = input.getFieldNames();
			for (String fieldName; it.hasNext();)
			{
				fieldName = it.next();
				if (strategy.inputCols.contains(fieldName))
					((ObjectNode) input).remove(fieldName); 
			}
			return input;
		}
		default:
			logger.warn("Unknown feature filter type "+type+" doing nothing");
			return input;
		}
		
	}

	
	
}
