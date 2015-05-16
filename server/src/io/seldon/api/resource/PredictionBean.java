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
package io.seldon.api.resource;


public class PredictionBean extends ResourceBean {

	double score;
	int predictedClass;
	double confidence;
	
	
	
	public PredictionBean(double score, int predictedClass, double confidence) {
		super();
		this.score = score;
		this.predictedClass = predictedClass;
		this.confidence = confidence;
	}



	public double getScore() {
		return score;
	}



	public int getPredictedClass() {
		return predictedClass;
	}



	public double getConfidence() {
		return confidence;
	}



	@Override
	public String toKey() {
		return this.hashCode()+"";
	}
	
}
