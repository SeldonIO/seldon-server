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

import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;

public class Prediction {

	
	//Prediction(consumer: String, rectag : String, variation : String, predictedClass : String, score : Double, time : Long, count : Int)
	
	String consumer;
	String variation;
	String predictedClass;
	String model;
	Double score;
	Long time;
	Integer count;
	
	public Prediction() 
	{
		consumer = "unknown";
		variation = "unknown";
		predictedClass = "unknown";
		model = "unknown";
	}
	
	public void parse(JsonNode j)
	{
		consumer = j.get("consumer").asText();
		time = j.get("time").asLong();
		count = 1;
		if (j.has("prediction"))
		{
			JsonNode prediction = j.get("prediction"); 
			if (prediction.has("meta"))
			{
				JsonNode meta = prediction.get("meta"); 
				if (meta.has("variation"))
					variation = meta.get("variation").asText();
				else
					variation = "default";
				if (meta.has("modelName"))
					model = meta.get("modelName").asText();
				else
					model = "default";
			}
			else
			{
				variation = "default";
				model = "default";
			}

			Iterator<JsonNode> iter = prediction.get("predictions").elements();
			double bestScore = 0;
			String bestClass = null;
			while (iter.hasNext())
			{
				JsonNode jPred = iter.next();
				double score = jPred.get("prediction").asDouble();
				String predClass = jPred.get("predictedClass").asText();
				if (bestClass == null || score > bestScore)
				{
					bestScore = score;
					bestClass = predClass;
				}
			}
			score = bestScore;
			predictedClass = bestClass;
		}
	}
	
	public Prediction add(Prediction other)
	{
		this.count += other.count;
		this.score += other.score;
		return this;
	}

	@Override
	public String toString() {
		return "Prediction [consumer=" + consumer 
				+ ", variation=" + variation + ", predictedClass="
				+ predictedClass + ", model=" + model + ", score=" + score
				+ ", time=" + time + ", count=" + count + "]";
	}

	
	
	
}
