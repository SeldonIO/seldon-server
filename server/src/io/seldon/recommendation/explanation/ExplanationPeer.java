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

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.ExceptionSwallowingMemcachedClient;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.UpdateRetriever;

@Component
public class ExplanationPeer {

    private static Logger logger = Logger.getLogger(ExplanationPeer.class.getName());

    private static String DEFAULT_LOCALE = "us-en";

    private ExplanationProvider defaultExplanationProvider;

    final static int EXPLANATION_CACHE_TIME_SECS = 20; // 3600;
    final private ExceptionSwallowingMemcachedClient memcacheClient;

    @Autowired
    public ExplanationPeer(ExceptionSwallowingMemcachedClient memcacheClient) {
        this.memcacheClient = memcacheClient;
        logger.info("initialized");
    }

    public String explainRecommendationResult(final String clientName, final String algKey, String localeIn) {
        
        final String locale;
        if (localeIn == null) {
            locale = DEFAULT_LOCALE;
            logger.debug(String.format("locale is null using defaults, locale[%s]", locale));
        } else {
            locale = localeIn;
        }

        String explanation;
        if (defaultExplanationProvider != null) {
            explanation = defaultExplanationProvider.getExplanation(algKey, locale);
        } else {

            boolean isCacheEnabled = true;

            ExplanationProvider explanationProvider;
            if (isCacheEnabled) {
                String memKey = MemCacheKeys.getExplanationsKey(clientName, algKey, locale);
                explanation = (String) memcacheClient.get(memKey);
                logger.debug(String.format("memKey[%s], recommendationExplanation[%s]", memKey, explanation));

                String newRes = null;
                try {
                    newRes = DogpileHandler.get().retrieveUpdateIfRequired(memKey, explanation, new UpdateRetriever<String>() {
                        @Override
                        public String retrieve() throws Exception {
                            SqlExplanationProvider sqlExplanationProvider = new SqlExplanationProvider(clientName);
                            return sqlExplanationProvider.getExplanation(algKey, locale);
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
                explanation = explanationProvider.getExplanation(algKey, locale);
            }
        }

        logger.debug(String.format("explaining [%s] as [%s]", algKey, explanation));
        return explanation;
    }

    public void setExplanationProvider(ExplanationProvider explanationProvider) {
        defaultExplanationProvider = explanationProvider;
    }

}
