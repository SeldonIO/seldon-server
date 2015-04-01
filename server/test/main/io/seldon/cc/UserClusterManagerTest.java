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

package io.seldon.cc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import io.seldon.resources.external.NewResourceNotifier;
import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import io.seldon.clustering.recommender.MemoryUserClusterStore;
import io.seldon.mf.PerClientExternalLocationListener;

public class UserClusterManagerTest {

	private NewResourceNotifier mockNewResourceNotifier;
	
	@Before
	public void createMocks()
	{
		mockNewResourceNotifier = createMock(NewResourceNotifier.class);
	}
	
	@Test
	public void testLoadClustersFromReader() throws IOException
	{
		String str = "{\"user\":50702,\"dim\":14,\"weight\":0.20000000298023224}\n"+
				"{\"user\":50702,\"dim\":72,\"weight\":0.13333334028720856}\n"+
				"{\"user\":50702,\"dim\":105,\"weight\":0.13333334028720856}\n"+
				"{\"user\":92548,\"dim\":36,\"weight\":0.24137930572032928}";
		
		InputStream is = new ByteArrayInputStream(str.getBytes());
		// read it with BufferedReader
		BufferedReader br = new BufferedReader(new InputStreamReader(is));

		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);

		
		UserClusterManager ucm = new UserClusterManager(null, mockNewResourceNotifier);
		
		MemoryUserClusterStore clusters = ucm.loadUserClusters("test", br);
		
		Assert.assertEquals(2, clusters.getNumUsersWithClusters());
	}
	
}
