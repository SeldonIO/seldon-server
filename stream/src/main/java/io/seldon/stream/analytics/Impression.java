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

import com.fasterxml.jackson.databind.JsonNode;

public class Impression {
	
	private String data;
	String consumer;
	String rectag;
	String variation;
	Long time;
	Integer imp = 0;
	Integer click = 0;
	
	public Impression(){}
	
	public Impression(JsonNode j)
	{
		consumer = j.get("consumer").asText();
		rectag = j.get("rectag").asText();
		variation = j.get("abkey").asText();
		time = j.get("time").asLong();
		String impType = j.get("click").asText();
		if (impType.equals("IMP"))
			imp = 1;
		else if (impType.equals("CTR"))
			click = 1;
	}

	public Impression add(Impression other)
	{
		imp += other.imp;
		click += other.click;
		return this;
	}

	@Override
	public String toString() {
		return "Impression [data=" + data + ", consumer=" + consumer
				+ ", rectag=" + rectag + ", variation=" + variation + ", time="
				+ time + ", imp=" + imp + ", click=" + click + "]";
	}
	
	
	

}
