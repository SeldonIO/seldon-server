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

package io.seldon.api.service;

import java.io.Serializable;

import io.seldon.trust.impl.CFAlgorithm;

//STORE TEST OPTIONS
public class ABTest implements Serializable {
	
	private CFAlgorithm algorithm;
	private double percentage; // 0->1
	
	public ABTest() {
	}
	
	
	public ABTest(CFAlgorithm algorithm, double percentage) {
		super();
		this.algorithm = algorithm;
		this.percentage = percentage;
	}

	

	public CFAlgorithm getAlgorithm() {
		return algorithm;
	}



	public void setAlgorithm(CFAlgorithm algorithm) {
		this.algorithm = algorithm;
	}



	public double getPercentage() {
		return percentage;
	}



	public void setPercentage(double percentage) {
		this.percentage = percentage;
	}


	public String toString()
	{
		return "Percentage:"+percentage+" Key:"+algorithm.getAbTestingKey()+" alg:"+algorithm.toString();
	}
}