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

package io.seldon.clustering.recommender;

import io.seldon.api.state.options.DefaultOptions;
import io.seldon.recommendation.AlgorithmStrategy;
import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.log4j.Logger;

import java.util.*;

/**
 *
 * Information that the Item Recommendation algorithms require to calculate what they should recommend. The main concept
 * is the 'mode'. Inclusion mode means that the context items are a set of items to recommend from, where as exclusion
 * mode means that they context items are a set of items to exclude from the recommendations.
 *
 * @author firemanphil
 *         Date: 25/11/14
 *         Time: 16:06
 */
public class RecommendationContext {

    private static final String ITEMS_PER_INCLUDER_OPTION_NAME = "io.seldon.algorithm.inclusion.itemsperincluder";
    private static Logger logger = Logger.getLogger(RecommendationContext.class.getName());
    private final String lastRecListUUID;



    private final OptionsHolder optsHolder;

    public enum MODE {
        INCLUSION, EXCLUSION, NONE
    }
    private final MODE mode;
    private final Set<Long> contextItems;
    private final Long currentItem;

    public RecommendationContext(MODE mode, Set<Long> contextItems, Long currentItem,
                                 String lastRecListUUID, OptionsHolder optsHolder) {
        logger.debug("Built new rec context object in mode " +mode.name());

        this.mode = mode;
        this.contextItems = contextItems;
        this.currentItem = currentItem;
        this.lastRecListUUID = lastRecListUUID;
        this.optsHolder = optsHolder;
    }

    public String getLastRecListUUID() {
        return lastRecListUUID;
    }

    public MODE getMode() {
        return mode;
    }

    public Set<Long> getContextItems() {
        return contextItems;
    }

    public Long getCurrentItem() {
        return currentItem;
    }
    public OptionsHolder getOptsHolder() {
        return optsHolder;
    }
    public static RecommendationContext buildContext(String client, AlgorithmStrategy strategy, Long user, String clientUserId,
                                                     Long currentItem, int dimensionId,
                                                     String lastRecListUUID, int numRecommendations,
                                                     DefaultOptions defaultOptions){

        OptionsHolder optsHolder = new OptionsHolder(defaultOptions, strategy.config);
        Set<Long> contextItems = new HashSet<>();

        Set<ItemIncluder> inclusionProducers = strategy.includers;
        Set<ItemFilter> itemFilters = strategy.filters;
        if(inclusionProducers == null || inclusionProducers.size() ==0){
            if (itemFilters==null || itemFilters.size() == 0){
                return new RecommendationContext(MODE.NONE, Collections.<Long>emptySet(), currentItem, lastRecListUUID,optsHolder);
            }
            for (ItemFilter filter : itemFilters){
                contextItems.addAll(filter.produceExcludedItems(client, user,clientUserId, optsHolder,currentItem, lastRecListUUID,
                        numRecommendations));
            }
            return new RecommendationContext(MODE.EXCLUSION, contextItems, currentItem, lastRecListUUID,optsHolder);
        }

        Integer itemsPerIncluder = optsHolder.getIntegerOption(ITEMS_PER_INCLUDER_OPTION_NAME);
        if(itemFilters == null || itemFilters.size() ==0) {
            for (ItemIncluder producer : inclusionProducers){
                contextItems.addAll(producer.generateIncludedItems(client, dimensionId,itemsPerIncluder));
            }
            return new RecommendationContext(MODE.INCLUSION, contextItems, currentItem, lastRecListUUID,optsHolder);
        }

        Set<Long> included = new HashSet<>();
        Set<Long> excluded = new HashSet<>();
        for (ItemFilter filter : itemFilters){
            excluded.addAll(filter.produceExcludedItems(client, user,clientUserId,optsHolder, currentItem, lastRecListUUID,
                    numRecommendations));
        }
        for (ItemIncluder producer : inclusionProducers){
            included.addAll(producer.generateIncludedItems(client, dimensionId, itemsPerIncluder));
        }
        included.removeAll(excluded); // ok to do this as the excluded items that weren't in "included" will never
                                      // be recommended

        return new RecommendationContext(MODE.INCLUSION, included, currentItem, lastRecListUUID,optsHolder);
    }

    public static class OptionsHolder{

        private final DefaultOptions defaultOptions;
        private final Map<String, String> options;

        public OptionsHolder(DefaultOptions options, Map<String, String > perStrategyOptions){
            this.defaultOptions = options;
            this.options = perStrategyOptions;
        }


        public String getStringOption(String optionName){
            if(options.containsKey(optionName))
                return options.get(optionName);
            return defaultOptions.getOption(optionName);
        }

        public boolean getBooleanOption(String optionName){

            if(options.containsKey(optionName))
                return BooleanUtils.toBoolean(options.get(optionName));
            else
                return BooleanUtils.toBoolean(defaultOptions.getOption(optionName));

        }

        public Double getDoubleOption(String optionName){
            try {
                if(options.containsKey(optionName))
                    return Double.parseDouble(options.get(optionName));
                else
                    return Double.parseDouble(defaultOptions.getOption(optionName));
            } catch (NumberFormatException | NullPointerException e){
                logger.error("Couldn't get algorithm option "+optionName,e);
                return 0.0D;
            }
        }

        public Integer getIntegerOption(String optionName){
            try {
                if(options.containsKey(optionName))
                    return Integer.parseInt(options.get(optionName));
                else
                    return Integer.parseInt(options.get(optionName));
            } catch (NumberFormatException | NullPointerException e){
                logger.error("Couldn't get algorithm option "+optionName,e);
                return 0;
            }
        }

    }





}
