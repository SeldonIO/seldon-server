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

package io.seldon.trust.impl;

public class TrustRecommendation implements Comparable<TrustRecommendation> {

	long content;
	int type;
	Double prediction;
	Double baseline;
	Long mostTrustedUser;
	Trust trust;
	
	public TrustRecommendation(long content, int type, Double prediction,
			Double baseline, Long mostTrustedUser, Trust trust) {
		this.content = content;
		this.prediction = prediction;
		this.baseline = baseline;
		this.mostTrustedUser = mostTrustedUser;
		this.trust = trust;
	}
	
	public long getContent() {
		return content;
	}

	public Double getPrediction() {
		return prediction;
	}

	public Double getBaseline() {
		return baseline;
	}

	public Long getMostTrustedUser() {
		return mostTrustedUser;
	}

	public Trust getTrust() {
		return trust;
	}

	public int getType() {
		return type;
	}

	@Override
	public int compareTo(TrustRecommendation o) {
		if (this.prediction > o.prediction)
			return -1;
		else if (this.prediction < o.prediction)
			return 1;
		else
			return 0;
	}
	
	
	
	
}
