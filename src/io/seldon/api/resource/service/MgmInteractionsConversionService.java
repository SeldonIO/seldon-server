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


import com.google.common.collect.Multimap;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ConversionBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import io.seldon.general.Interaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author philipince
 *         Date: 30/01/2014
 *         Time: 16:06
 */
@Component
public class MgmInteractionsConversionService extends ConversionService{

    private static final Logger logger = LoggerFactory.getLogger(MgmInteractionsConversionService.class);
    private final Integer defaultDaysOfHistoryToConsider;

    private class ConversionCandidate {

        public final String consumerName;
        public final UserBean user;

        private ConversionCandidate(String consumerName, UserBean user) {
            this.consumerName = consumerName;
            this.user = user;
        }
    }


    private final InteractionService interactionService;
    private final ExternalUserService extUsrService;
    private final Boolean useAmazonQueue;
    private final ConcurrentMap<String,ConcurrentMap<String, ConversionCandidate>> fbIdToCandidate = new ConcurrentHashMap<String, ConcurrentMap<String,ConversionCandidate>>();
    private ConcurrentMap<String,BlockingQueue<ConversionCandidate>> toCheckQueues = new ConcurrentHashMap<String, BlockingQueue<ConversionCandidate>>();
    private ConcurrentMap<String, Integer> consumerToDays = new ConcurrentHashMap<String, Integer>();

    @Autowired
    public MgmInteractionsConversionService(@Value("${interactions.conversion.use.queue:false}") Boolean useAmazonQueue,
                                            InteractionService interactionService,
                                            MultiVariateTestStore store,
                                            @Value("${conversions.history.days:14}") Integer daysOfHistory, ExternalUserService extUsrService){
        super(store);
        this.extUsrService = extUsrService;
        logger.info("Starting up...");
        this.useAmazonQueue = useAmazonQueue;
        this.interactionService = interactionService;
        this.defaultDaysOfHistoryToConsider = daysOfHistory;
    }


    public void submitForConversionCheck(ConsumerBean consumer, UserBean bean, int daysOfHistoryToConsider) {
        logger.info("Received user "+ bean.getId() +" to check for conversion for consumer " + consumer.getShort_name());
        toCheckQueues.putIfAbsent(consumer.getShort_name(), new LinkedBlockingDeque<ConversionCandidate>());
        fbIdToCandidate.putIfAbsent(consumer.getShort_name(), new ConcurrentHashMap<String, ConversionCandidate>());

        BlockingQueue<ConversionCandidate> queue = toCheckQueues.get(consumer.getShort_name());
        ConcurrentMap<String, ConversionCandidate> fbIdToCandidateMap = fbIdToCandidate.get(consumer.getShort_name());

        ConversionCandidate candidate = new ConversionCandidate(consumer.getShort_name(), bean);
        queue.add(candidate);
        fbIdToCandidateMap.putIfAbsent(bean.getAttributesName().get(FBConstants.FB_ID),candidate);
        int size = queue.size();
        if(size>50){
            logger.info("Queue size is "+size + ", draining 50 users to check for conversion.");
            Set<ConversionCandidate> candidatesToCheck = new HashSet<ConversionCandidate>(50);
            queue.drainTo(candidatesToCheck, 50);
            doCheck(candidatesToCheck, consumer.getShort_name(), daysOfHistoryToConsider);
        }
    }

    @Scheduled(fixedRate = 30*1000)
    public void timedCheck(){
        logger.info("Checking queues for stragglers");
        for(String consumer: toCheckQueues.keySet()){
            BlockingQueue<ConversionCandidate> queue = toCheckQueues.get(consumer);
            while(queue.size()>0){
                logger.info(consumer + " queue size = "+queue.size());
                Set<ConversionCandidate> candidatesToCheck = new HashSet<ConversionCandidate>(50);
                queue.drainTo(candidatesToCheck, 50);
                doCheck(candidatesToCheck, consumer, defaultDaysOfHistoryToConsider);
            }
        }
    }

    private void doCheck(Set<ConversionCandidate> candidatesToCheck, String consumer, int daysOfHistoryToConsider){
        Collection<UserBean> usersToCheck = new ArrayList<UserBean>();
        for(ConversionCandidate candidateToTest : candidatesToCheck){
            usersToCheck.add(candidateToTest.user);
        }
        Multimap<String, Interaction> interactions =interactionService.retrieveUnconvertedInteractionsBySecondaryUser(
                consumer,usersToCheck,
                InteractionService.MGM_TYPE, InteractionService.InteractionSubType.INVITE.ordinal()+1);
        int interactionsCount = interactions.size();
        logger.info("Found "+ interactionsCount +" interactions with " + interactions.keySet().size() +
                " unique invited users for a batch of " + candidatesToCheck.size() + " users on consumer "+ consumer);
        long theFuture = System.currentTimeMillis() - (daysOfHistoryToConsider * 24 * 60 * 60 * 1000);
        Date twoWeeksAgo = new Date(theFuture);
        interactions = filterByDate(twoWeeksAgo, interactions);
        int filtered = (interactionsCount-interactions.size());
        if(filtered!=0){
            logger.info("Filtering "+filtered+ " interactions due to date constraints.");
        }
        Map<Interaction, Boolean> interactionToIsAppUser =
                extUsrService.retrieveAttrByInteraction(consumer, interactions.values());

        Set<String> keys = interactions.keySet();
        for(String key : keys){
            List<Interaction> interactionList = new ArrayList<Interaction>(interactions.get(key));
            if(interactionList == null || interactionList.isEmpty()){
                continue;
            }
            // convert most recent interaction
            Collections.sort(interactionList, new Comparator<Interaction>() {
                @Override
                public int compare(Interaction o1, Interaction o2) {
                    return o2.getDate().compareTo(o1.getDate());
                }
            });
            Interaction interaction = interactionList.remove(0);
            String u1Id = interaction.getUser1Id();
            Boolean isAppUser = interactionToIsAppUser.get(interaction);
            isAppUser = isAppUser==null? true : isAppUser;
            registerConversion(consumer, new ConversionBean(
                    isAppUser ?
                            ConversionBean.ConversionType.EXISTING_USER : ConversionBean.ConversionType.NEW_USER,
                    u1Id,fbIdToCandidate.get(consumer).get(interaction.getUser2FbId()).user.getId(), new Date())
            );
            interactionService.addConversionEvent(consumer, interaction);
            for(Interaction otherInteraction : interactionList) {
                interactionService.addInactivatedEvent(consumer, otherInteraction);
            }
        }
    }

    private Multimap<String, Interaction> filterByDate(Date date, Multimap<String, Interaction> interactions) {

            Iterator<Map.Entry<String, Interaction>> iter = interactions.entries().iterator();
            while (iter.hasNext()) {
                Interaction interaction = iter.next().getValue();
                if (!interaction.getDate().after(date)) {
                    iter.remove();
                }
            }

        return interactions;
    }

}
