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

package io.seldon.topics;

import static org.easymock.EasyMock.createMock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.NewResourceNotifier;
import io.seldon.util.CollectionTools;

import org.junit.Before;
import org.junit.Test;


public class TopicFeaturesManagerTest implements PerClientExternalLocationListener {

	private NewResourceNotifier mockNewResourceNotifier;
	
	@Before
	public void createMocks()
	{
		mockNewResourceNotifier = createMock(NewResourceNotifier.class);
	}
	
	@Test
	public void testTreeSet()
	{
		Set<Long> ids1 = new HashSet<Long>();
		ids1.add(3L);
		ids1.add(6L);
		ids1.add(1L);
		ids1.add(100L);
		String v =  CollectionTools.join(new ArrayList<Long>(new TreeSet<Long>(ids1)), ",");
		System.out.println("v is "+v);
	}
	


	@Override
	public void newClientLocation(String client, String location,String nodePattern) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clientLocationDeleted(String client,String nodePattern) {
		// TODO Auto-generated method stub
		
	}
	
}
