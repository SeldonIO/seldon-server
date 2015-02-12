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

package io.seldon.mgm.keyword;

import java.util.concurrent.ConcurrentHashMap;

public class MgmKeywordConfBean {

	private ConcurrentHashMap<String,String> clientToLanguages;
	private ConcurrentHashMap<String,String> clientToType;
	private String defaultType;
	private String defaultLangs;

	
	public MgmKeywordConfBean() {}
	
	public ConcurrentHashMap<String, String> getClientToLanguages() {
		return clientToLanguages;
	}




	public void setClientToLanguages(
			ConcurrentHashMap<String, String> clientToLanguages) {
		this.clientToLanguages = clientToLanguages;
	}




	public ConcurrentHashMap<String, String> getClientToType() {
		return clientToType;
	}




	public void setClientToType(ConcurrentHashMap<String, String> clientToType) {
		this.clientToType = clientToType;
	}




	public String getDefaultType() {
		return defaultType;
	}




	public void setDefaultType(String defaultType) {
		this.defaultType = defaultType;
	}




	public String getDefaultLangs() {
		return defaultLangs;
	}




	public void setDefaultLangs(String defaultLangs) {
		this.defaultLangs = defaultLangs;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		
	}

}
