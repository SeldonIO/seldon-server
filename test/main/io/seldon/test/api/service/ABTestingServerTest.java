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

package io.seldon.test.api.service;

import java.util.Random;

import io.seldon.api.service.ABTest;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import junit.framework.Assert;

import org.junit.Test;

import io.seldon.api.service.ABTestingServer;

public class ABTestingServerTest extends BasePeerTest {

	@Test
	public void setGetTest()
	{
		final String clientName = "Test";
		final String recTag = "tag1";
		CFAlgorithm alg = new CFAlgorithm();
		alg.setName(clientName);
		alg.setRecTag(recTag);
		alg.setAbTestingKey("B");
		ABTest abTest = new ABTest(alg,1.0);
		
		ABTestingServer.setABTest(clientName, abTest);
		
		ABTest abTest2 = ABTestingServer.getABTest(clientName, recTag);
		
		Assert.assertEquals(abTest, abTest2);

		Random r = new Random();
		final String userId = "test"+r.nextInt();
		CFAlgorithm algTest = ABTestingServer.getUserTest(clientName, recTag, userId);
		
		Assert.assertEquals(alg, algTest);
		
		CFAlgorithm algTest2 = ABTestingServer.getUserTest(clientName, null, userId);

		Assert.assertNull(algTest2); // no alg without tag
	}
	
	
}
