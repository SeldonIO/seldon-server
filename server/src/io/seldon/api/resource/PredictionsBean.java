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

import java.util.ArrayList;
import java.util.List;

public class PredictionsBean extends ResourceBean {

	String model;
	String abkey;
	String upid;
	List<PredictionBean> results;
	
	public PredictionsBean()
	{
		results = new ArrayList<PredictionBean>();
	}
	
	
	
	public PredictionsBean(String model, String abkey, String upid,
			List<PredictionBean> results) {
		super();
		this.model = model;
		this.abkey = abkey;
		this.upid = upid;
		this.results = results;
	}



	public String getModel() {
		return model;
	}



	public void setModel(String model) {
		this.model = model;
	}



	public String getAbkey() {
		return abkey;
	}



	public void setAbkey(String abkey) {
		this.abkey = abkey;
	}



	public String getUpid() {
		return upid;
	}



	public void setUpid(String upid) {
		this.upid = upid;
	}



	public List<PredictionBean> getResults() {
		return results;
	}



	public void setResults(List<PredictionBean> results) {
		this.results = results;
	}



	@Override
	public String toKey() {
		return this.hashCode()+"";
	}

}
