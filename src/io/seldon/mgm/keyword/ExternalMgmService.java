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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


public abstract class ExternalMgmService {

	private static Logger logger = Logger.getLogger(ExternalMgmService.class.getName());
	
	protected final String KeywordPath = "/query/";
	protected final String PARAM_USER = "user";
	protected final String PARAM_KEYWORD = "keywords";
	protected final String PARAM_LANG = "languages";
	protected final String PARAM_CLIENT = "client";
	
	String baseUrl;
	
	public ExternalMgmService(String baseUrl)
	{
		this.baseUrl = baseUrl;
	}
	
	protected String getUrl()
	{
		return baseUrl;
	}
	
	public abstract String callService(long user,String[] keywords,String[] languages,String client);

	
	protected String getPath(long user,String[] keywords,String[] languages,String client)
	{
		try
		{
			StringBuffer url = new StringBuffer();
			url.append(KeywordPath);
			url.append("?");
			url.append(PARAM_USER).append("=").append(user).append("&");
			url.append(PARAM_CLIENT).append("=").append(client).append("&");
			url.append(PARAM_KEYWORD).append("=").append(URLEncoder.encode(StringUtils.join(keywords, ","),"UTF-8")).append("&");
			url.append(PARAM_LANG).append("=").append(StringUtils.join(languages, ","));
			return url.toString();
		} 
		catch (UnsupportedEncodingException e) 
		{
			logger.error("Failed to create url ",e);
			return null;
		}
		
	}
	
}
