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
package io.seldon.mf;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class MfUserClustersModelManagerTest {
	private NewResourceNotifier mockNewResourceNotifier;
	
	@Before
	public void createMocks()
	{
		mockNewResourceNotifier = createMock(NewResourceNotifier.class);
	}
	
	@Test
	public void testLoadUserClustersFromReader() throws IOException
	{
		String str = "1,1\n2,2\n3,3";

		
		InputStream is = new ByteArrayInputStream(str.getBytes());
		// read it with BufferedReader
		BufferedReader br = new BufferedReader(new InputStreamReader(is));

		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		
		MfUserClustersModelManager m = new MfUserClustersModelManager(null, mockNewResourceNotifier);
		Map<Long,Integer> clusters = m.createUserClusters(br);
		Integer cid = clusters.get(1L);
		Assert.assertNotNull(clusters);
		Assert.assertEquals(3, clusters.size());
		Assert.assertEquals((Integer)1, clusters.get(1));
	}
}
