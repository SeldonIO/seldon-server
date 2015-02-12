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

package io.seldon.test.storm;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;

import io.seldon.storm.DRPCSettings;
import io.seldon.storm.DRPCSettingsFactory;

public class DRPCSettingsFactoryTest {

	@Test
	public void propsTest()
	{
		Properties p = new Properties();
		final String client = "TEST";
		final String host = "localhost";
		final int port = 1234;
		final int timeout = 5678;
		final String topologyName = "abcd";
		p.setProperty(DRPCSettingsFactory.PROP_PREFIX+"clients", client);
		p.setProperty(DRPCSettingsFactory.PROP_PREFIX+client+DRPCSettingsFactory.DRPC_HOST_SUFFIX, host);
		p.setProperty(DRPCSettingsFactory.PROP_PREFIX+client+DRPCSettingsFactory.DRPC_PORT_SUFFIX, ""+port);
		p.setProperty(DRPCSettingsFactory.PROP_PREFIX+client+DRPCSettingsFactory.DRPC_TIMEOUT_SUFFIX, ""+timeout);
		p.setProperty(DRPCSettingsFactory.PROP_PREFIX+client+DRPCSettingsFactory.DRPC_REC_SUFFIX, topologyName);
		
		DRPCSettingsFactory.initialise(p);
		
		DRPCSettings s = DRPCSettingsFactory.get(client);
		
		Assert.assertNotNull(s);
		Assert.assertEquals(host, s.getHost());
		Assert.assertEquals(port, s.getPort());
		Assert.assertEquals(timeout, s.getTimeout());
		Assert.assertEquals(topologyName, s.getRecommendationTopologyName());
	}
}
