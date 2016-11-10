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

package io.seldon.recommendation.explanation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.ExceptionSwallowingMemcachedClient;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.UpdateRetriever;

@Component
public class ExplanationPeer implements ClientConfigUpdateListener {

    private static Logger logger = Logger.getLogger(ExplanationPeer.class.getName());

    public static class RecommendationExplanationConfig {
        public boolean cache_enabled = false;
        public String default_locale = "us-en";
        public boolean explanations_enabled = false;

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    private final static String RECOMMENDATION_EXPLANATION_MAPPING_KEY = "recommendation_explanation";
    private ObjectMapper objMapper = new ObjectMapper();

    private ExplanationProvider defaultExplanationProvider;

    final static int EXPLANATION_CACHE_TIME_SECS = 3600;
    final private ExceptionSwallowingMemcachedClient memcacheClient;

    private Map<String, RecommendationExplanationConfig> client_recommendation_explanation_configs = new ConcurrentHashMap<>();

    private final static String DEFAULT_EXPLANATION = ""; // use this incase of a null explanation

    @Autowired
    public ExplanationPeer(ExceptionSwallowingMemcachedClient memcacheClient, ClientConfigHandler configHandler) {
        this.memcacheClient = memcacheClient;
        if (configHandler != null) {
            configHandler.addListener(this);
        }
        logger.info("initialized");
    }

    public boolean isExplanationNeededForClient(String clientName) {
        boolean retVal = false;

        RecommendationExplanationConfig recommendationExplanationConfig = client_recommendation_explanation_configs.get(clientName);
        if ((recommendationExplanationConfig != null) && (recommendationExplanationConfig.explanations_enabled)) {
            retVal = true;
        }

        return retVal;
    }

    public String explainRecommendationResult(final String clientName, String recommenderIn, String localeIn) {

        final String recommender = normalizeRecommender(recommenderIn);

        RecommendationExplanationConfig recommendationExplanationConfig = client_recommendation_explanation_configs.get(clientName);
        if ((recommendationExplanationConfig == null) || (!recommendationExplanationConfig.explanations_enabled)) {
            return "Explanation not enbaled for client";
        }

        final String locale;
        if (localeIn == null) {
            locale = recommendationExplanationConfig.default_locale;
            logger.debug(String.format("locale is null using defaults, locale[%s]", locale));
        } else {
            locale = localeIn;
        }

        String explanation;
        if (defaultExplanationProvider != null) {
            explanation = defaultExplanationProvider.getExplanation(recommender, locale);
        } else {

            ExplanationProvider explanationProvider;
            if (recommendationExplanationConfig.cache_enabled) {
                String memKey = MemCacheKeys.getExplanationsKey(clientName, recommender, locale);
                explanation = (String) memcacheClient.get(memKey);
                logger.debug(String.format("memKey[%s], recommendationExplanation[%s]", memKey, explanation));

                String newRes = null;
                try {
                    newRes = DogpileHandler.get().retrieveUpdateIfRequired(memKey, explanation, new UpdateRetriever<String>() {
                        @Override
                        public String retrieve() throws Exception {
                            SqlExplanationProvider sqlExplanationProvider = new SqlExplanationProvider(clientName);
                            return sqlExplanationProvider.getExplanation(recommender, locale);
                        }
                    }, EXPLANATION_CACHE_TIME_SECS);
                } catch (Exception e) {
                    logger.warn("Error when retrieving static recommendations in dogpile handler ", e);
                }
                if (newRes != null) {
                    memcacheClient.set(memKey, EXPLANATION_CACHE_TIME_SECS, newRes);
                    explanation = newRes;
                }

            } else {
                explanationProvider = new SqlExplanationProvider(clientName);
                explanation = explanationProvider.getExplanation(recommender, locale);
            }
        }

        if (explanation == null) {
            explanation = DEFAULT_EXPLANATION;
        }

        logger.debug(String.format("explaining [%s] as [%s]", recommender, explanation));
        return explanation;
    }

    public void setExplanationProvider(ExplanationProvider explanationProvider) {
        defaultExplanationProvider = explanationProvider;
    }

    private RecommendationExplanationConfig getRecommendationExplanationConfigFromJson(String json) throws IOException {
        RecommendationExplanationConfig recommendationExplanationConfig = null;
        recommendationExplanationConfig = objMapper.readValue(json, RecommendationExplanationConfig.class);
        return recommendationExplanationConfig;
    }

    public void updateRecommendationExplanationConfig(String client, String json) {

        try {
            RecommendationExplanationConfig recommendationExplanationConfig = getRecommendationExplanationConfigFromJson(json);
            client_recommendation_explanation_configs.put(client, recommendationExplanationConfig);
            logger.info(String.format("Updated recommendation explanation config for client[%s] value[%s]", client, recommendationExplanationConfig));
        } catch (Exception e) {
            logger.error(String.format("Failed to update recommendation explanation config using json[%s]", json), e);
        }
    }

    public void removeRecommendationExplanationConfig(String client) {
        client_recommendation_explanation_configs.remove(client);
        logger.info(String.format("Removed recommendation explanation config for client[%s]", client));
    }

    @Override
    public void configUpdated(String client, String configKey, String configValue) {
        if (configKey.equals(RECOMMENDATION_EXPLANATION_MAPPING_KEY)) {
            logger.info("Received new recommendation explanation config for " + client + ": " + configValue);
            updateRecommendationExplanationConfig(client, configValue);
        }
    }

    @Override
    public void configRemoved(String client, String configKey) {
        if (configKey.equals(RECOMMENDATION_EXPLANATION_MAPPING_KEY)) {
            removeRecommendationExplanationConfig(client);
        }

    }

    public static String normalizeRecommender(String recommender) {
        String retVal = recommender;

        int n = recommender.indexOf(':');
        if (n >= 0) {
            retVal = retVal.substring(0, n);
        }

        return retVal;
    }
}
