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

package io.seldon.api.resource;

import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class ItemSimilarityNodeBean extends ResourceBean {
	
	String item;
	double sim;
	
	ItemSimilarityNodeBean() {
	}

	public ItemSimilarityNodeBean(String id, double sim) {
		item = id;
		this.sim = sim;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public double getSim() {
		return sim;
	}

	public void setSim(double similarity) {
		this.sim = similarity;
	}

	@Override
	public String toKey() {
		return item;
	}

	public int compareTo(ItemSimilarityNodeBean o) {
		if(this.sim == o.sim)
			return this.item.compareTo(o.item);
		else if(this.sim > o.sim)
			return -1;
		else 
			return 1;
	}
}
