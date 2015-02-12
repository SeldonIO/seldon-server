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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.seldon.api.AlgorithmService;
import io.seldon.api.Util;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.caching.ClientIdCacheStore;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendationBean;
import io.seldon.api.resource.RecommendationsBean;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
//import static org.easymock.EasyMock.*;

public class RemoveHistoryForSortsTest  extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	
	
	@Test
	public void removeHistoryTrue() throws CloneNotSupportedException
	{
		
		CFAlgorithm options = new CFAlgorithm();
		options.setName(props.getClient());
		List<CFAlgorithm.CF_SORTER> sorters = new ArrayList<CFAlgorithm.CF_SORTER>();
		sorters.add(CFAlgorithm.CF_SORTER.COOCCURRENCE);
		options.setSorters(sorters);
		options.setPostprocessing(CFAlgorithm.CF_POSTPROCESSING.ADD_MISSING);
		options.setRankingRemoveHistory(true);
		
		final long userId = 1L;
		final String clientUserId = "user1";
		final long item1 = 1L;
		final String clientItem1 = "item1";
		final long item2 = 2L;
		final String clientItem2 = "item2";
		
		AlgorithmService origService = Util.getAlgorithmService();
		
		try
		{
		//Create algorithm options and store for use
		final ConsumerBean cbean = new ConsumerBean(props.getClient());
		AlgorithmService as = createMock(AlgorithmService.class);
		expect(as.getAlgorithmOptions(cbean)).andReturn(options);
		expect(as.getAlgorithmOptions(props.getClient())).andReturn(options);
		replay(as);
		Util.setAlgorithmService(as);
		
		//Create user history
		ActionHistoryCache ah = new ActionHistoryCache(props.getClient());
		ah.removeRecentActions(userId);
		ah.addAction(userId, 1L);

		Properties p = new Properties();
		p.setProperty("io.seldon.idstore.clients", props.getClient());
		ClientIdCacheStore.initialise(p);
		
		//set client user id mapping
		ClientIdCacheStore.get().putUserId(props.getClient(), clientUserId, userId);
		//set client item id mapping
		ClientIdCacheStore.get().putItemId(props.getClient(), clientItem1, item1);
		ClientIdCacheStore.get().putItemId(props.getClient(), clientItem2, item2);
		
		RecommendationsBean rbean = new RecommendationsBean();
		List<RecommendationBean> itemList = new ArrayList<RecommendationBean>();
		itemList.add(new RecommendationBean(clientItem1,1,null));
		itemList.add(new RecommendationBean(clientItem2,2,null));
		rbean.setList(itemList);
		Object[] res = RecommendationService.sort(cbean, clientUserId, rbean, null);
		
		RecommendationsBean result = (RecommendationsBean) res[0];
		Assert.assertEquals(2, result.getList().size());
		Assert.assertEquals(clientItem2, result.getList().get(0).getItem());
		Assert.assertEquals(clientItem1, result.getList().get(1).getItem()); //removed item added to end of rankings
		}
		finally
		{
		Util.setAlgorithmService(origService);
		}
	}
	
	@Test
	public void removeHistoryFalse() throws CloneNotSupportedException
	{
		
		CFAlgorithm options = new CFAlgorithm();
		options.setName(props.getClient());
		List<CFAlgorithm.CF_SORTER> sorters = new ArrayList<CFAlgorithm.CF_SORTER>();
		sorters.add(CFAlgorithm.CF_SORTER.COOCCURRENCE);
		options.setSorters(sorters);
		options.setPostprocessing(CFAlgorithm.CF_POSTPROCESSING.ADD_MISSING);
		options.setRankingRemoveHistory(false);
		
		final long userId = 1L;
		final String clientUserId = "user1";
		final long item1 = 1L;
		final String clientItem1 = "item1";
		final long item2 = 2L;
		final String clientItem2 = "item2";
		
		AlgorithmService origService = Util.getAlgorithmService();
		
		try
		{
		//Create algorithm options and store for use
		final ConsumerBean cbean = new ConsumerBean(props.getClient());
		AlgorithmService as = createMock(AlgorithmService.class);
		expect(as.getAlgorithmOptions(cbean)).andReturn(options);
		expect(as.getAlgorithmOptions(props.getClient())).andReturn(options);
		replay(as);
		Util.setAlgorithmService(as);
		
		//Create user history
		ActionHistoryCache ah = new ActionHistoryCache(props.getClient());
		ah.removeRecentActions(userId);
		ah.addAction(userId, 1L);

		Properties p = new Properties();
		p.setProperty("io.seldon.idstore.clients", props.getClient());
		ClientIdCacheStore.initialise(p);
		
		//set client user id mapping
		ClientIdCacheStore.get().putUserId(props.getClient(), clientUserId, userId);
		//set client item id mapping
		ClientIdCacheStore.get().putItemId(props.getClient(), clientItem1, item1);
		ClientIdCacheStore.get().putItemId(props.getClient(), clientItem2, item2);
		
		RecommendationsBean rbean = new RecommendationsBean(); 
		List<RecommendationBean> itemList = new ArrayList<RecommendationBean>();
		itemList.add(new RecommendationBean(clientItem1,1,null));
		itemList.add(new RecommendationBean(clientItem2,2,null));
		rbean.setList(itemList);
		Object[] res = RecommendationService.sort(cbean, clientUserId, rbean, null);
		
		RecommendationsBean result = (RecommendationsBean) res[0];
		Assert.assertEquals(2, result.getList().size());
		Assert.assertEquals(clientItem1, result.getList().get(0).getItem());
		Assert.assertEquals(clientItem2, result.getList().get(1).getItem()); 
		}
		finally
		{
		Util.setAlgorithmService(origService);
		}
	}

}
