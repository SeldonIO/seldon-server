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

package io.seldon.test.service;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Date;

import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.InteractionBean;
import io.seldon.api.resource.service.PersistenceProvider;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.SecurityHashPeer;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.seldon.api.resource.service.InteractionService;
import io.seldon.general.InteractionPeer;

public class InteractionServiceTest {

    private MultiVariateTestStore mockTestStore;

    private static class MemCachePeerMock extends MemCachePeer {
        public static void setClient(MemcachedClient mockClient){
            client = mockClient;
        }
    }

    private String keyString;
    private Date date;
    private int type;
    private int subType;
    private long user1;
    private String clientName;
    private ConsumerBean consumerBean;
    private String user1String;
    private String user2String;
    //private UserServiceAdapter mockUserService;
    private MemcachedClient mockMemCacheClient;
    private PersistenceProvider mockPersistenceProvider;
    private InteractionPeer mockInteractionPeer;
    private String beansKeyString;
    private MemcachedClient realClient;
    
    @Before
    public void setUp(){
        user1 = 1L;
        user1String = "user1";
        user2String = "user2";
        type = 1 ;
        subType = 1;
        date = new Date();
        keyString = "InteractionBean:1:2:1:1";
        beansKeyString = "InteractionsBean:1:type"; 
        clientName = "name";
        consumerBean = new ConsumerBean(clientName);

        mockMemCacheClient = createMock(MemcachedClient.class);
        mockPersistenceProvider = createMock(PersistenceProvider.class); 
        mockInteractionPeer = createMock(InteractionPeer.class);
        realClient = MemCachePeer.getClient();
        mockTestStore = createMock(MultiVariateTestStore.class);
        MemCachePeerMock.setClient(mockMemCacheClient);
    }
    
    @After
    public void tearDown(){
        MemCachePeerMock.setClient(realClient);
    }
    
    @Test
    public void shouldAddToDbAndCacheIfItDoesntExistAnywhere(){
        InteractionService serviceToTest = new InteractionService(mockPersistenceProvider,  mockTestStore, null,null);
        InteractionBean interactionBean = new InteractionBean(user1String, user2String, type, subType, date);

//        expect(mockUserService.toInternalUserId(consumerBean, user1String)).andReturn(user1);
//        expect(mockMemCacheClient.get(SecurityHashPeer.md5digest(keyString))).andReturn(null);
//        expect(mockMemCacheClient.set(SecurityHashPeer.md5digest(keyString), 3600, interactionBean)).andReturn((OperationFuture)anyObject());
//        expect(mockPersistenceProvider.getInteractionPersister(clientName)).andReturn(mockInteractionPeer);
//        expect(mockInteractionPeer.getInteractions(user1, type)).andReturn(null);
//        mockInteractionPeer.saveOrUpdateInteraction(eq(new Interaction(user1, user2String, type, subType, date)));
        replay(mockMemCacheClient, mockInteractionPeer, mockPersistenceProvider);
        
        serviceToTest.addInteraction(consumerBean, interactionBean);
        
        verify(mockMemCacheClient, mockInteractionPeer, mockPersistenceProvider);
        
    }
    
    @Test
    public void shouldDoNothingIfBeanAlreadyInCache(){
        InteractionService serviceToTest = new InteractionService(null,  mockTestStore, null, null);

        InteractionBean interactionBean = new InteractionBean(user1String, user2String, type, subType, date);
        expect(mockMemCacheClient.get(SecurityHashPeer.md5digest(keyString))).andReturn(interactionBean);

//        expect(mockUserService.toInternalUserId(consumerBean, user1String)).andReturn(user1);
        
        replay(mockMemCacheClient);
        serviceToTest.addInteraction(consumerBean, interactionBean);

        verify(mockMemCacheClient);        
    }
    
    @Test
    public void shouldAddToCacheIfBeanInDbButNotCache(){
        InteractionService serviceToTest = new InteractionService(mockPersistenceProvider,  mockTestStore, null, null);
        
        InteractionBean interactionBean = new InteractionBean(user1String, user2String, type, subType, date);

//        expect(mockUserService.toInternalUserId(consumerBean, user1String)).andReturn(user1);
        expect(mockMemCacheClient.get(SecurityHashPeer.md5digest(keyString))).andReturn(null);
        expect(mockMemCacheClient.set(SecurityHashPeer.md5digest(keyString), 3600, interactionBean)).andReturn((OperationFuture)anyObject());
        expect(mockPersistenceProvider.getInteractionPersister(clientName)).andReturn(mockInteractionPeer);
        //expect(mockInteractionPeer.getInteractions(user1, type)).andReturn(Arrays.asSet(new Interaction(user1, user2String, type, subType, date)));
        
        replay(mockMemCacheClient, mockInteractionPeer, mockPersistenceProvider);
        
        serviceToTest.addInteraction(consumerBean, interactionBean);
        
        verify(mockMemCacheClient, mockInteractionPeer, mockPersistenceProvider);
    }
    
  
}
