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
package io.seldon.ar;

import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;
import io.seldon.tags.UserTagAffinityManager;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class AssocRuleManager extends ModelManager<AssocRuleManager.AssocRuleStore> {

    private static Logger logger = Logger.getLogger(UserTagAffinityManager.class.getName());
    private final ExternalResourceStreamer featuresFileHandler;
    public static final String ASSOC_NEW_LOC_PATTERN = "assocrules";


    @Autowired
    public AssocRuleManager(ExternalResourceStreamer featuresFileHandler, NewResourceNotifier notifier) {
        super(notifier, Collections.singleton(ASSOC_NEW_LOC_PATTERN));
        this.featuresFileHandler = featuresFileHandler;
    }


    protected AssocRuleStore loadAssocRules(String client, BufferedReader reader) throws IOException {
        String line;
        ObjectMapper mapper = new ObjectMapper();
        int ruleId = 0;

        Map<Integer, AssocRuleRecommendation> assocRules = new HashMap<Integer, AssocRuleRecommendation>();
        Map<Long, Map<Integer, Set<Integer>>> itemToRules = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            AssocRule data = mapper.readValue(line.getBytes(), AssocRule.class);
            ruleId++;
            double score = data.confidence * data.lift * data.itemset.size();
            assocRules.put(ruleId, new AssocRuleRecommendation(data.item, score));
            int antecedentLen = data.itemset.size();
            for (Long item : data.itemset) {
                Map<Integer, Set<Integer>> lenToRules = itemToRules.get(item);
                if (lenToRules == null) {
                    lenToRules = new HashMap<Integer, Set<Integer>>();
                    itemToRules.put(item, lenToRules);
                }
                Set<Integer> rules = lenToRules.get(antecedentLen);
                if (rules == null) {
                    rules = new HashSet<Integer>();
                    lenToRules.put(antecedentLen, rules);
                }
                rules.add(ruleId);
            }
        }

        return new AssocRuleStore(assocRules, itemToRules);

    }


    @Override
    protected AssocRuleStore loadModel(String location, String client) {
        logger.info("Reloading user assoc rules for client: " + client);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(featuresFileHandler.getResourceStream(location + "/part-00000")))) {
            AssocRuleStore store = loadAssocRules(client, reader);

            logger.info("finished load of " + store.assocRules.size() + " user assoc rules for client " + client);
            return store;
        } catch (IOException e) {
            logger.error("Couldn't reloadFeatures for client " + client, e);

            logger.error("Couldn't reloadFeatures for client " + client, e);
        }

        return null;
    }


    public static class AssocRuleRecommendation {
        public final Long item;
        public final Double score;

        public AssocRuleRecommendation(Long item, Double score) {
            super();
            this.item = item;
            this.score = score;
        }
    }


    public static class AssocRuleStore {

        public final Map<Integer, AssocRuleRecommendation> assocRules; // rule-id to rule recommednation
        public final Map<Long, Map<Integer, Set<Integer>>> itemToRules; // antecedent item -> (map rule-size -> set of rules-ids)

        public AssocRuleStore(Map<Integer, AssocRuleRecommendation> assocRules,
                              Map<Long, Map<Integer, Set<Integer>>> itemToRules) {
            super();
            this.assocRules = assocRules;
            this.itemToRules = itemToRules;
        }
    }


}

