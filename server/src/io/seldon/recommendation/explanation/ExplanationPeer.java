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
import org.springframework.stereotype.Component;

@Component
public class ExplanationPeer {

    private static Logger logger = Logger.getLogger(ExplanationPeer.class.getName());

    private static String DEFAULT_LOCALE = "us-en";

    private ExplanationProvider defaultExplanationProvider;

    public ExplanationPeer() {
        logger.info("initialized");
    }

    public String explainRecommendationResult(String clientName, String algKey, String locale) {

        if (locale == null) {
            locale = DEFAULT_LOCALE;
            logger.debug(String.format("locale is null using defaults, locale[%s]", locale));
        }

        String explanation;
        if (defaultExplanationProvider != null) {
            explanation = defaultExplanationProvider.getExplanation(algKey, locale);
        } else {
            ExplanationProvider explanationProvider = new SqlExplanationProvider(clientName);
            explanation = explanationProvider.getExplanation(algKey, locale);
        }

        logger.debug(String.format("explaining [%s] as [%s]", algKey, explanation));
        return explanation;
    }

    public void setExplanationProvider(ExplanationProvider explanationProvider) {
        defaultExplanationProvider = explanationProvider;
    }
}
