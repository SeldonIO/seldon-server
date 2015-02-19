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

package io.seldon.test.trust.impl;

import java.util.ArrayList;
import java.util.List;

import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.trust.impl.SortResult;

public class AddMissingPostProcessorTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Test
	public void checkAddMissing() throws CloneNotSupportedException
	{
		
		CFAlgorithm options = new CFAlgorithm();
		options.setName(props.getClient());
		List<CFAlgorithm.CF_SORTER> sorters = new ArrayList<>();
		sorters.add(CFAlgorithm.CF_SORTER.COOCCURRENCE);
		options.setSorters(sorters);
		options.setPostprocessing(CFAlgorithm.CF_POSTPROCESSING.NONE);
		RecommendationPeer recPeer = new RecommendationPeer();
		
		final long userId = 1L;
		List<Long> items = new ArrayList<>();
		items.add(1L);
		items.add(2L);
		items.add(3L);
		SortResult r = recPeer.sort(userId, items, options, new ArrayList<Long>());
		
		Assert.assertEquals(0, r.getSortedItems().size());
		
		options.setPostprocessing(CFAlgorithm.CF_POSTPROCESSING.ADD_MISSING);
		r = recPeer.sort(userId, items, options, new ArrayList<Long>());
		
		Assert.assertEquals(3, r.getSortedItems().size());
		Assert.assertEquals(new Long(1), r.getSortedItems().get(0));
		Assert.assertEquals(new Long(2), r.getSortedItems().get(1));
		Assert.assertEquals(new Long(3), r.getSortedItems().get(2));
	}
}
