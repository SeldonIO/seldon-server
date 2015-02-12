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

import static com.restfb.util.StringUtils.fromInputStream;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.log4j.Logger;

public class SimpleMgmKeywordClient extends ExternalMgmService {

	private static Logger logger = Logger.getLogger(SimpleMgmKeywordClient.class.getName());
	
	
	
	public SimpleMgmKeywordClient(String baseUrl)
	{
		super(baseUrl);
	}
	
	 protected HttpURLConnection openConnection(URL url) throws IOException 
	 {
		 return (HttpURLConnection) url.openConnection();
	 }
	
	 protected void closeQuietly(HttpURLConnection httpUrlConnection) 
	 {
		 if (httpUrlConnection == null)
			 return;
		 try 
		 {
			 httpUrlConnection.disconnect();
		 } 
		 catch (Throwable t) 
		 {
		     logger.error("Problem closing connection ",t);
		 }
	 }
	  
	 public String callService(long user,String[] keywords,String[] languages,String client) {
		
		 try
		 {
			 String url = getUrl()+getPath(user, keywords, languages, client);
			 if (url != null)
				 return callServiceImpl(url);
			 else
				 return null;
		 }
		 catch (IOException e)
		 {
			 logger.error("Failed call ",e);
			 return null;
		 }
	 }
	 
	 public String callServiceImpl(String url) throws IOException {
		    HttpURLConnection httpUrlConnection = null;
		    InputStream inputStream = null;

		    try {
		      httpUrlConnection = openConnection(new URL(url));
		      httpUrlConnection.setReadTimeout(10000);
		      httpUrlConnection.setUseCaches(false);


		      httpUrlConnection.setRequestMethod("GET");
		      httpUrlConnection.connect();

		      
		      try {
		        inputStream =
		            httpUrlConnection.getResponseCode() != HTTP_OK ? httpUrlConnection.getErrorStream() : httpUrlConnection
		              .getInputStream();
		      } catch (IOException e) 
		      {
		    	  logger.error("Failed to get response ",e);
		      }

		      return fromInputStream(inputStream);
		    } finally {
		      closeQuietly(httpUrlConnection);
		    }
		  }
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		//String url = "http://localhost:10000/query/?user=604041084&keyword=david%20bowie&language=en&client=rockol_it";
		SimpleMgmKeywordClient s = new SimpleMgmKeywordClient("http://localhost:10000");
		String[] keywords = new String[]{"david bowie"};
		String[] languages = new String[] {"en"};
		String r = s.callService(604041084, keywords, languages, "rockol_it");
		System.out.println(r);

	}

	

}
