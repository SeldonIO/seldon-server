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

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

@Service
public class MgmKeywordConfService {

	private static final String DB = "db";
	private static final String SERVICE = "service";
	
	@Resource(name = "mgmKeywordServiceEndpoint")
	private volatile String endpoint;
	@Resource(name = "mgmKeywordServiceNameToLanguages")
	private volatile ConcurrentHashMap<String,String> clientToLanguages;
	@Resource(name = "mgmKeywordClientType")
	private volatile ConcurrentHashMap<String,String> clientToType;
	@Resource(name = "mgmKeywordDefaultClientType")
	private volatile String defaultType;
	@Resource(name = "mgmKeywordDefaultClientLangs")
	private volatile String defaultLangs;
	
	
	
	void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	public String getEndpoint() {
		return endpoint;
	}
	public String[] getLanguages(String client) {
		if (clientToLanguages.containsKey(client))
			return clientToLanguages.get(client).split(",");
		else
			return defaultLangs.split(",");
	}
	
	public boolean isDBClient(String client)
	{
		String ty = clientToType.get(client);
		if (ty != null)
			return DB.equals(ty);
		else
			return DB.equals(defaultType);
	}
	
	public void updateConf(MgmKeywordConfBean b)
	{
		if (b.getClientToLanguages() != null)
			clientToLanguages = b.getClientToLanguages();
		if (b.getClientToType() != null)
			clientToType = b.getClientToType();
		if (!StringUtils.isEmpty(b.getDefaultLangs()))
			defaultLangs = b.getDefaultLangs();
		if (!StringUtils.isEmpty(b.getDefaultType()))
			defaultType = b.getDefaultType();
	}
}
