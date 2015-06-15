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
package io.seldon.ar;

import java.util.List;

public class AssocRule {

	List<Long> itemset;
	Long item;
	double confidence;
	double lift;
	double interest;
	
	public AssocRule(){}
	
	public AssocRule(List<Long> itemset, Long item, double confidence,
			double lift, double interest) {
		super();
		this.itemset = itemset;
		this.item = item;
		this.confidence = confidence;
		this.lift = lift;
		this.interest = interest;
	}
	public List<Long> getItemset() {
		return itemset;
	}
	public Long getItem() {
		return item;
	}
	public double getConfidence() {
		return confidence;
	}
	public double getLift() {
		return lift;
	}
	public double getInterest() {
		return interest;
	}
	
	
}
