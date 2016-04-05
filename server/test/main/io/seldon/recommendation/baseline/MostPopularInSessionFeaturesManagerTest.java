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
package io.seldon.recommendation.baseline;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import io.seldon.api.resource.service.ItemService;
import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.recommendation.baseline.MostPopularInSessionFeaturesManager.DimPopularityStore;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class MostPopularInSessionFeaturesManagerTest {
	private NewResourceNotifier mockNewResourceNotifier;
	private ItemService mockItemService;
	
	@Before
	public void createMocks()
	{
		mockNewResourceNotifier = createMock(NewResourceNotifier.class);
	}
	
	@Test
	public void testLoadClustersFromReader() throws IOException
	{
		String str = "{\"dim\":1,\"item\":1,\"count\":10.0}\n"+
				"{\"dim\":1,\"item\":2,\"count\":8.0}\n"+
				"{\"dim\":2,\"item\":3,\"count\":5.0}\n"+
				"{\"dim\":2,\"item\":4,\"count\":2.0}\n";
		
		InputStream is = new ByteArrayInputStream(str.getBytes());
		// read it with BufferedReader
		BufferedReader br = new BufferedReader(new InputStreamReader(is));

		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		
		MostPopularInSessionFeaturesManager m = new MostPopularInSessionFeaturesManager(null, mockNewResourceNotifier);
		DimPopularityStore store = m.createStore(br, "test");
		Assert.assertEquals(2,store.dimToPopularItems.size());
		Assert.assertEquals(2,store.getTopItemsForDimension(1).size());
		Assert.assertEquals(2,store.getTopItemsForDimension(2).size());
		Assert.assertNull(store.getTopItemsForDimension(0));
	}
}
