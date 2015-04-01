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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

public class GeneralSubCategoryExtractor implements CategoryExtractor{
	private static String result="";
	private static Logger logger = Logger.getLogger(GeneralAllCategoryExtractor.class.getName());
	static final Pattern pattern = Pattern.compile("\\d{4}|\\w{1,15}_\\w{1,15}_\\w{1,15}_[a-zA-Z0-9- ./_<>?;!@#$%^=&*]*|\\w{1,15}-\\w{1,15}-\\w{1,15}-[a-zA-Z0-9- ./_<>?;!@#$%^=&*]*");
	
	@Override
	public String getCategory(String url, Document document){
		String domain ="";
		String result ="";
		try {
			domain = getDomainName(url);
		} catch (MalformedURLException e) {
			return null;
		}
		int start = url.indexOf(domain) + domain.length() + 1;
		if (start>0 && start<url.length()){
			url = url.substring(start);
			url = url.substring(0, url.length());
			//logger.info("Returning section "+url);

			String urlsubcat = url.replaceAll("//","/");	//eliminate the double // in the url
			if(urlsubcat.startsWith("/")){
				urlsubcat = urlsubcat.substring(1,url.length());
			}
			String urlsplit[] = urlsubcat.split("/");
			int i = 0;
			while( i<urlsplit.length ){
				Matcher mat = pattern.matcher(urlsplit[i]);
				if( mat.matches()){
					break;
				}
				result += urlsplit[i]+"/";
				i++;
				}
		return result;
		}
	return null;
	}
	
	public String getDomainName(String url) throws MalformedURLException{
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
