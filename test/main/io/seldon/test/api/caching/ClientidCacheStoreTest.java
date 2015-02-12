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

package io.seldon.test.api.caching;

import java.util.Properties;
import java.util.Random;

import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.caching.ClientIdCacheStore;

public class ClientidCacheStoreTest  extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Test
	public void testUnmappedClient()
	{
		Properties idProps = new Properties();
		ClientIdCacheStore.initialise(idProps);
		
		Random r = new Random();
		String client = props.getClient();
		String external  = ""+r.nextInt();
		Long internal = r.nextLong();
		String resExt = ClientIdCacheStore.get().getExternalItemId(client, internal);
		Assert.assertNull(resExt);
		Long resInt = ClientIdCacheStore.get().getInternalItemId(client, external);
		Assert.assertNull(resInt);
		resExt = ClientIdCacheStore.get().getExternalUserId(client, internal);
		Assert.assertNull(resExt);
		resInt = ClientIdCacheStore.get().getInternalUserId(client, external);
		Assert.assertNull(resInt);
		ClientIdCacheStore.get().putItemId(client, external, internal);
		resExt = ClientIdCacheStore.get().getExternalItemId(client, internal);
		Assert.assertNull(resExt);
		ClientIdCacheStore.get().putUserId(client, external, internal);
		resExt = ClientIdCacheStore.get().getExternalUserId(client, internal);
		Assert.assertNull(resExt);

	}
	
	@Test
	public void testAddGetUserId()
	{
		Properties idProps = new Properties();
		idProps.put("io.seldon.idstore.clients", props.getClient());
		ClientIdCacheStore.initialise(idProps);
		
		String external = "ext1";
		Long internal = 1L;
		ClientIdCacheStore.get().putUserId(props.getClient(), external, internal);
		
		Long v = ClientIdCacheStore.get().getInternalUserId(props.getClient(), external);
		Assert.assertEquals(internal, v);
		
		String s = ClientIdCacheStore.get().getExternalUserId(props.getClient(), internal);
		Assert.assertEquals(external, s);
	}
	
	@Test
	public void testAddGetItemId()
	{
		Properties idProps = new Properties();
		idProps.put("io.seldon.idstore.clients", props.getClient());
		ClientIdCacheStore.initialise(idProps);
		
		String external = "item1";
		Long internal = 2L;
		ClientIdCacheStore.get().putItemId(props.getClient(), external, internal);
		
		Long v = ClientIdCacheStore.get().getInternalItemId(props.getClient(), external);
		Assert.assertEquals(internal, v);
		
		String s = ClientIdCacheStore.get().getExternalItemId(props.getClient(), internal);
		Assert.assertEquals(external, s);
	}
	
	@Test 
	public void testMaximumSize()
	{
		Properties idProps = new Properties();
		idProps.put("io.seldon.idstore.clients", props.getClient());
		idProps.put("io.seldon.idstore."+props.getClient()+".maxitems", "1");
		idProps.put("io.seldon.idstore."+props.getClient()+".maxusers", "1");
		ClientIdCacheStore.initialise(idProps);
		
		String e1 = "item1";
		Long i1 = 1L;
		String e2 = "item2";
		Long i2 = 2L;
		ClientIdCacheStore.get().putItemId(props.getClient(), e1, i1);
		ClientIdCacheStore.get().putItemId(props.getClient(), e2, i2);
		
		Long v = ClientIdCacheStore.get().getInternalItemId(props.getClient(), e1);
		Assert.assertNull(v);
		v = ClientIdCacheStore.get().getInternalItemId(props.getClient(), e2);
		Assert.assertNotNull(v);
	}
	
}
