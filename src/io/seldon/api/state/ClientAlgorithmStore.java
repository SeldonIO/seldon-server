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

package io.seldon.api.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.recommendation.AlgorithmStrategy;
import io.seldon.recommendation.ClientStrategy;
import io.seldon.recommendation.SimpleClientStrategy;
import io.seldon.recommendation.VariationTestingClientStrategy;
import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;
import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;
import io.seldon.trust.impl.filters.base.CurrentItemFilter;
import io.seldon.trust.impl.filters.base.IgnoredRecsFilter;
import io.seldon.trust.impl.filters.base.RecentImpressionsFilter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache of which algorithms to use for which clients. Receives updates via ClientConfigUpdateListener
 *
 * @author firemanphil
 *         Date: 27/11/14
 *         Time: 13:55
 */
@Component
public class ClientAlgorithmStore implements ApplicationContextAware,ClientConfigUpdateListener,GlobalConfigUpdateListener {

    protected static Logger logger = Logger.getLogger(ClientAlgorithmStore.class.getName());

    private static final String ALG_KEY = "algs";
    private static final String TESTING_SWITCH_KEY = "alg_test_switch";
    private static final String TEST = "alg_test";



    private final ClientConfigHandler configHandler;
    private final GlobalConfigHandler globalConfigHandler;
    private final Set<ItemFilter> alwaysOnFilters;
    private ApplicationContext applicationContext;
    private ConcurrentMap<String, ClientStrategy> store = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Map<String, AlgorithmStrategy>> storeMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Boolean> testingOnOff = new ConcurrentHashMap<>();
    private ConcurrentMap<String, ClientStrategy> tests = new ConcurrentHashMap<>();
    private ClientStrategy defaultStrategy = null;
    private ConcurrentMap<String, ClientStrategy> namedStrategies = new ConcurrentHashMap<>();


    public Map<String, AlgorithmStrategy> getAlgorithmsByTag(String client){
        return storeMap.get(client);
    }

    @Autowired
    public ClientAlgorithmStore (ClientConfigHandler configHandler,
                                 GlobalConfigHandler globalConfigHandler,
                                 CurrentItemFilter currentItemFilter,
                                 IgnoredRecsFilter ignoredRecsFilter,
                                 RecentImpressionsFilter recentImpressionsFilter){
        this.configHandler = configHandler;
        this.globalConfigHandler = globalConfigHandler;
        Set<ItemFilter> set = new HashSet<>();
        set.add (currentItemFilter);
        set.add(ignoredRecsFilter);
        set.add(recentImpressionsFilter);
        alwaysOnFilters = Collections.unmodifiableSet(set);
    }

    @PostConstruct
    private void init(){
        logger.info("Initializing...");
        configHandler.addListener(this);
        globalConfigHandler.addSubscriber("default_strategy", this);
//        globalConfigHandler.addSubscriber("named_strategies", this);
    }

    public ClientStrategy retrieveStrategy(String client){
        if (testRunning(client)){
            ClientStrategy strategy = tests.get(client);
            if(strategy!=null){
                return strategy;
            } else {
                logger.warn("Testing was switch on for client " + client+ " but no test was specified." +
                        " Returning default strategy");
                return defaultStrategy;
            }
        } else {
            ClientStrategy strategy = store.get(client);
            if(strategy!=null){
                return strategy;
            } else {
                return defaultStrategy;
            }
        }
    }



    @Override
    public void configUpdated(String client, String configKey, String configValue) {
        if (configKey.equals(ALG_KEY)){
            logger.info("Received new algorithm config for "+ client+": "+ configValue);
            try {
                ObjectMapper mapper = new ObjectMapper();
                List<AlgorithmStrategy> strategies = new ArrayList<>();
                Map<String, AlgorithmStrategy> stratMap = new HashMap<>();
                AlgorithmConfig config = mapper.readValue(configValue, AlgorithmConfig.class);
                for (Algorithm algorithm : config.algorithms) {
                    AlgorithmStrategy strategy = toAlgorithmStrategy(algorithm,"-");
                    strategies.add(strategy);
                    stratMap.put(algorithm.tag, strategy);
                }
                AlgorithmResultsCombiner combiner = applicationContext.getBean(
                        config.combiner,AlgorithmResultsCombiner.class);
                store.put(client, new SimpleClientStrategy(Collections.unmodifiableList(strategies), combiner));
                storeMap.put(client, Collections.unmodifiableMap(stratMap));
                logger.info("Successfully added new algorithm config for "+client);
            } catch (IOException | BeansException e) {
                logger.error("Couldn't update algorithms for client " +client, e);
            }
        } else if (configKey.equals(TESTING_SWITCH_KEY)){
            // not json as its so simple
            Boolean onOff = BooleanUtils.toBooleanObject(configValue);
            if(onOff==null){
                logger.error("Couldn't set testing switch for client "+client +", input was " +configValue);
            } else {
                logger.info("Testing switch for client " + client + " moving from '" +
                        BooleanUtils.toStringOnOff(testingOnOff.get(client)) +
                        "' to '" + BooleanUtils.toStringOnOff(onOff)+"'");
                testingOnOff.put(client, BooleanUtils.toBooleanObject(configValue));
            }
        } else {
            if (configKey.equals(TEST)) {
                logger.info("Received new testing config for " + client + ":" + configValue);
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    TestConfig config = mapper.readValue(configValue, TestConfig.class);
                    Set<VariationTestingClientStrategy.Variation> variations = new HashSet<>();
                    for (TestVariation var : config.variations){
                        List<AlgorithmStrategy> strategies = new ArrayList<>();
                        for (Algorithm alg : var.config.algorithms){
                            AlgorithmStrategy strategy = toAlgorithmStrategy(alg,var.label);
                            strategies.add(strategy);
                        }
                        AlgorithmResultsCombiner combiner = applicationContext.getBean(
                                var.config.combiner,AlgorithmResultsCombiner.class);
                        variations.add(new VariationTestingClientStrategy.Variation(
                                new SimpleClientStrategy(Collections.unmodifiableList(strategies), combiner),
                                new BigDecimal(var.ratio)));

                    }
                    tests.put(client,VariationTestingClientStrategy.build(variations));
                    logger.info("Succesfully added " + variations.size() + " variation test for "+ client);
                } catch (NumberFormatException | IOException e) {
                    logger.error("Couldn't add test for client " +client, e);
                }
            }
        }
    }

    private AlgorithmStrategy toAlgorithmStrategy(Algorithm algorithm, String name) {
        Set<ItemIncluder> includers = retrieveIncluders(algorithm.includers);
        Set<ItemFilter> filters = retrieveFilters(algorithm.filters);
        ItemRecommendationAlgorithm alg = applicationContext.getBean(algorithm.name, ItemRecommendationAlgorithm.class);
        int itemsPerIncluder = Integer.parseInt(algorithm.config.get("items_per_includer"));
        return new AlgorithmStrategy(alg, includers, filters, itemsPerIncluder, name);
    }

    private Set<ItemIncluder> retrieveIncluders(List<String> includers) {
        Set<ItemIncluder> includerSet = new HashSet<>();
        for (String includer : includers){
            includerSet.add(applicationContext.getBean(includer, ItemIncluder.class));
        }
        return includerSet;
    }

    private Set<ItemFilter> retrieveFilters(List<String> filters) {
        Set<ItemFilter> filterSet = new HashSet<>();
        for (String filter : filters){
            filterSet.add(applicationContext.getBean(filter, ItemFilter.class));
        }
        return Sets.union(filterSet, alwaysOnFilters);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        StringBuilder builder= new StringBuilder("Available algorithms: \n");
        for (ItemRecommendationAlgorithm inc : applicationContext.getBeansOfType(ItemRecommendationAlgorithm.class).values()){
            builder.append('\t');
            builder.append(inc.getClass());
            builder.append('\n');
        }
        logger.info(builder.toString());
        builder= new StringBuilder("Available includers: \n");
        for (ItemIncluder inc : applicationContext.getBeansOfType(ItemIncluder.class).values()){
            builder.append('\t');
            builder.append(inc.getClass());
            builder.append('\n');
        }
        logger.info(builder.toString());
        builder = new StringBuilder("Available filters: \n" );
        for (ItemFilter filt: applicationContext.getBeansOfType(ItemFilter.class).values()){
            builder.append('\t');
            builder.append(filt.getClass());
            builder.append('\n');

        }
        logger.info(builder.toString());
        for (AlgorithmResultsCombiner filt: applicationContext.getBeansOfType(AlgorithmResultsCombiner.class).values()){
            builder.append('\t');
            builder.append(filt.getClass());
            builder.append('\n');

        }
        builder = new StringBuilder("Available combiners: \n" );
        logger.info(builder.toString());
    }

    private boolean testRunning(String client){
        return testingOnOff.get(client)!=null && testingOnOff.get(client) && tests.get(client)!=null;
    }

    @Override
    public void configUpdated(String configKey, String configValue) {
        logger.info("KEY WAS " + configKey);
        logger.info("Received new default strategy: " + configValue);
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<AlgorithmStrategy> strategies = new ArrayList<>();
            AlgorithmConfig config = mapper.readValue(configValue, AlgorithmConfig.class);

            for (Algorithm alg : config.algorithms){
                strategies.add(toAlgorithmStrategy(alg, alg.tag));
            }
            AlgorithmResultsCombiner combiner = applicationContext.getBean(
                    config.combiner,AlgorithmResultsCombiner.class);
            ClientStrategy strat = new SimpleClientStrategy(strategies, combiner);
            defaultStrategy = strat;
            logger.info("Successfully changed default strategy.");
        } catch (IOException e){
            logger.error("Problem changing default strategy ", e);
        }
    }

    // classes for json translation
    public static class TestConfig {
        public Set<TestVariation> variations;
    }

    private static class TestVariation {
        public String label;
        public String ratio;
        public AlgorithmConfig config;
    }
    public static class AlgorithmConfig {
        public List<Algorithm> algorithms;
        public String combiner;
    }

    public static class Algorithm {

        public String name;
        public String tag;
        public List<String> includers;
        public List<String> filters;
        public Map<String, String> config;
    }

}