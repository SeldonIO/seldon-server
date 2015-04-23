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

package io.seldon.recommendation;

import java.util.List;

public class SortResult {

	private List<Long> sortedItems;
	private List<CFAlgorithm.CF_SORTER> sorters;
	private CFAlgorithm.CF_STRATEGY sorterStrategy;

	public SortResult(List<Long> sortedItems, List<CFAlgorithm.CF_SORTER> sorters,
			CFAlgorithm.CF_STRATEGY sorterStrategy) {
		super();
		this.sortedItems = sortedItems;
		this.sorters = sorters;
		this.sorterStrategy = sorterStrategy;
	}

	public List<Long> getSortedItems() {
		return sortedItems;
	}

	public List<CFAlgorithm.CF_SORTER> getSorters() {
		return sorters;
	}

	public CFAlgorithm.CF_STRATEGY getSorterStrategy() {
		return sorterStrategy;
	}

	public String toLog() {
		String res = "";
		//CF_SORTER
		if(sorters!=null && sorters.size()>0) {
			for(CFAlgorithm.CF_SORTER s : sorters) {
				res += s.name() + "|";
			}
			res = res.substring(0,res.length()-1);
		}
		//CF_STRATEGY
		if(sorterStrategy!=null) 
			res +=";" + sorterStrategy.name();
		return res;
	}
	
}
