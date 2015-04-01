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
package io.seldon.importer.articles.category;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

public class GeneralAllCategoryExtractor  implements CategoryExtractor {

	private static Logger logger = Logger.getLogger(GeneralAllCategoryExtractor.class.getName());
	
	public GeneralAllCategoryExtractor(){}

	@Override
	public String getCategory(String url, Document document) {
		String domain;
		try {
			domain = getDomainName(url);
		} catch (MalformedURLException e) {
			return null;
		}
		int start = url.indexOf(domain) +  domain.length() + 1;
		if (start>0 && start<url.length()){
			url = url.substring(start);
			String section = url.substring(0,url.lastIndexOf("/"));
			logger.info("Returning section "+section);
			return section;
		}
		logger.warn("No section found in "+url);
		return null;
	}
	
	public static String getDomainName(String url) throws MalformedURLException{
	    if(!url.startsWith("http") && !url.startsWith("https")){
	         url = "http://" + url;
	    }        
	    URL netUrl = new URL(url);
	    String host = netUrl.getHost();
	    if(host.startsWith("www")){
	        host = host.substring("www".length()+1);
	    }
	    return host;
	}

}

