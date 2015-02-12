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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.thrift7.TException;
import org.junit.Test;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

import io.seldon.storm.DRPCSettings;
import io.seldon.storm.TrustDRPCRecommender;
import io.seldon.util.CollectionTools;

public class TrustDRPCRecommenderTest {

	@Test
	public void simpleTestWithMockDRPC() throws TException, DRPCExecutionException
	{
		final String topology = "t";
		List<Long> itemsToSort = new ArrayList<Long>();
		itemsToSort.add(1L);
		itemsToSort.add(2L);
		final long userId = 1L;
		final int dimension = -1;
		final String result = "1,2,3,4,5";
		DRPCClient drpcClient = StormTestUtils.getMockDRPCClient(topology, TrustDRPCRecommender.getSortMessage(userId, dimension, itemsToSort), result);
		DRPCSettings s = new DRPCSettings("host", 1234, 1234, topology);
		
		TrustDRPCRecommender r = new TrustDRPCRecommender(s, drpcClient);
		
		List<Long> sorted = r.sort(userId,dimension,itemsToSort);
		
		String sortedAsString = CollectionTools.join(sorted, ",");
		
		Assert.assertEquals(result, sortedAsString);
	}
	
}
