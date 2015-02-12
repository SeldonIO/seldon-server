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

package io.seldon.api.resource.service;

import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.seldon.api.logging.MgmLogger;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.client.AsyncFacebookClient;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.InteractionBean;
import io.seldon.general.*;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.seldon.api.Constants;


@Service
public class InteractionService {

    private static Logger logger = Logger.getLogger(InteractionService.class.getName());
    public static final int MGM_TYPE = 1;
    @Autowired
    private CacheExpireService cacheExpireService;


    public void addConversionEvent(String consumer, Interaction interaction) {
        persister.getInteractionPersister(consumer).addInteractionEvent(interaction,
                new InteractionEvent(InteractionEventType.CONVERTED.getTypeNumber()));
    }

    public void addInactivatedEvent(String consumer, Interaction interaction){
        persister.getInteractionPersister(consumer).addInteractionEvent(interaction,
                new InteractionEvent(InteractionEventType.INACTIVE.getTypeNumber()));
    }

    public static enum InteractionSubType {
        INVITE,
        DELETE
    }

    private PersistenceProvider persister;
    private MultiVariateTestStore testStore;
    private final ExternalUserService extUsrService;


    @Autowired
    public InteractionService(PersistenceProvider persister,
                              MultiVariateTestStore testStore,
                              AsyncFacebookClient fbClient, ExternalUserService extUsrService){
        this.persister = persister;
        this.testStore = testStore;
        this.extUsrService = extUsrService;
    }

    public void addInteraction(ConsumerBean consumerBean, InteractionBean interactionBean){
        String shortName = consumerBean.getShort_name();

        // get ALL interactions for interacting user of correct type so we can cache them
        Set<Interaction> interactions = doRetrieveInteractions(consumerBean.getShort_name(),
                interactionBean.getUser1(), interactionBean.getType());
        InteractionPeer interactionPeer = persister.getInteractionPersister(consumerBean.getShort_name());

        Interaction interaction = interactionBean.toInteraction(interactionBean.getUser1());

        // finally we can add the item
        interactionPeer.saveOrUpdateInteraction(interaction);
        logger.debug("Added interaction for "+shortName+" " +ToStringBuilder.reflectionToString(interaction));

        boolean isAppUsr = extUsrService.deriveAndAddAttrsFromInteraction(consumerBean, interactionBean);

        Iterator<Interaction> iter = interactions.iterator();
        while(iter.hasNext()){
            Interaction interactionToTest = iter.next();
            if(interactionBean.getUser2().equals(interactionToTest.getUser2FbId())){
                iter.remove();
                break;
            }
        }
        interactions.add(interaction);
        String interactionsByTypeBeanKey = MemCacheKeys.getInteractionsBeanKey(consumerBean.getShort_name(),
                interactionBean.getUser1(), interactionBean.getType());
        if(Constants.CACHING) MemCachePeer.put(interactionsByTypeBeanKey, interactions, cacheExpireService.getCacheExpireSecs());
        registerAction(consumerBean, interactionBean, isAppUsr);
    }

    public Set<Interaction> retrieveInteractions(ConsumerBean consumer, String clientUserId, int type){
        String interactionsByTypeBeanKey = MemCacheKeys.getInteractionsBeanKey(consumer.getShort_name(),
                clientUserId, type);
        Set<Interaction> interactions = doRetrieveInteractions(consumer.getShort_name(), clientUserId, type);
        if(Constants.CACHING) MemCachePeer.put(interactionsByTypeBeanKey, interactions, cacheExpireService.getCacheExpireSecs());
        return interactions;
    }

    private Set<Interaction> doRetrieveInteractions(String consumerName, String userId, int type){
        String interactionsByTypeBeanKey = MemCacheKeys.getInteractionsBeanKey(consumerName, userId, type);
        Set<Interaction> interactions = (Set<Interaction>) MemCachePeer.get(interactionsByTypeBeanKey);
        if(interactions!=null) return interactions;
        InteractionPeer interactionPeer = persister.getInteractionPersister(consumerName);
        return interactionPeer.getInteractions(userId,type);
    }

    public Multimap<String, Interaction> retrieveUnconvertedInteractionsBySecondaryUser(String consumerName, Collection<UserBean> users, int type, int subType){
        Multimap<String, Interaction> toReturn = HashMultimap.create();
        Collection<String> fbIds = new HashSet<String>();
        for(UserBean user : users){
            fbIds.add(user.getAttributesName().get(FBConstants.FB_ID));
        }
        Set<Interaction> interactions = persister.getInteractionPersister(consumerName).getUnconvertedInteractionsByInteractedWithUsers(fbIds, type, subType);
        for(Interaction interaction : interactions){
            toReturn.put(interaction.getUser2FbId(), interaction);
        }
        return toReturn;
    }

    private void registerAction(ConsumerBean consumerBean, InteractionBean interactionBean, boolean isAppUsr) {
        if(interactionBean.getSubType()==(InteractionSubType.INVITE.ordinal()+1)){
            MgmAction action = new MgmAction(interactionBean.getUser1(), interactionBean.getUser2(),
                    new Date(),
                    isAppUsr? MgmAction.MgmActionType.EXISTING_USR_SHARE : MgmAction.MgmActionType.NEW_USR_SHARE,
                    null, null);

            // inform any running stats processes
            String mvTestVariationKey = null;
            if(testStore.testRunning(consumerBean.getShort_name())){
                testStore.registerTestEvent(consumerBean.getShort_name(), action);
                mvTestVariationKey = testStore.retrieveVariationKey(consumerBean.getShort_name(), interactionBean.getUser1());
            }

            // log action
            MgmLogger.log(consumerBean.getShort_name(), action, mvTestVariationKey);
        }
    }

    ;





}
