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

import java.util.ArrayList;
import java.util.List;

import io.seldon.api.Constants;


public class RecommendationsBean extends ResourceBean {
	
	protected long size;
	protected long requested;
	protected List<RecommendationBean> list = new ArrayList<RecommendationBean>();

	public List<RecommendationBean> getList() {
		return list;
	}

	public void setList(List<RecommendationBean> list) {
		this.list = list;
	}
	
	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getRequested() {
		return requested;
	}

	public void setRequested(long requested) {
		this.requested = requested;
	}

	public void addBean(RecommendationBean r) { this.list.add(r); }

	@Override
	public String toKey() {
		return this.hashCode()+"";
	}
	
	@Override
	public String toLog() {
		if(list != null) {
			String items = "";
			int count = 0;
			for(RecommendationBean r : list) {
				items += r.item +"|";
				if(++count>Constants.LIST_LOG_LIMIT) { break; }
			}
			if(count>0) { items = items.substring(0,items.length()-1); }
			return list.size()+";"+requested+";"+items;
		}
		else {
			return "0;" + requested+";";
		}
	}
	
	public List<String> toItems() {
		ArrayList<String> res = new ArrayList<String>();
		for(RecommendationBean r : list) {
			res.add(r.getItem());
		}
		return res;
	}
}
