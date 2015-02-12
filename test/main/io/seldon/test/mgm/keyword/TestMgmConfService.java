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

package io.seldon.test.mgm.keyword;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.seldon.api.Util;
import io.seldon.mgm.keyword.MgmKeywordConfService;
import io.seldon.mgm.keyword.ZkMgmKeywordConfUpdater;
import io.seldon.test.BaseTest;
import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

import io.seldon.mgm.keyword.MgmKeywordConfBean;

@ContextConfiguration({"classpath*:/test-mgm-service-ctx.xml"})
public class TestMgmConfService extends BaseTest {

	
	
	@Test
	public void testEndpointUpdate() throws JsonParseException, JsonMappingException, IOException
	{
		String endpoint = "newendpoint";
		ZkMgmKeywordConfUpdater u = new ZkMgmKeywordConfUpdater();
		u.childEventUpdate("/" + ZkMgmKeywordConfUpdater.path+"/" + ZkMgmKeywordConfUpdater.endpointPath,endpoint.getBytes());
		Assert.assertEquals(endpoint, Util.getMgmKeywordConf().getEndpoint());
	}
	
	@Test
	public void testLangUpdate() throws JsonParseException, JsonMappingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		MgmKeywordConfBean b = new MgmKeywordConfBean();
		ConcurrentHashMap<String,String> m = new ConcurrentHashMap<String, String>();
		String client = "testClient";
		String langs = "en,es";
		m.put(client, langs);
		b.setClientToLanguages(m);
		String val = mapper.writeValueAsString(b);
		ZkMgmKeywordConfUpdater u = new ZkMgmKeywordConfUpdater();
		u.childEventUpdate("/" + ZkMgmKeywordConfUpdater.path+"/" + ZkMgmKeywordConfUpdater.confPath,val.getBytes());
		Assert.assertEquals(langs, StringUtils.join(Util.getMgmKeywordConf().getLanguages(client),","));
	}
	
	@Test
	public void getVals()
	{
		MgmKeywordConfService c = Util.getMgmKeywordConf();
		System.out.println(c.getEndpoint());
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
