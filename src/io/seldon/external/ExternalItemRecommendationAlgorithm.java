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

package io.seldon.external;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * An Item Recommendation Algorithm that calls out to an HTTP endpoint for its recs.
 *
 * @author firemanphil
 *         Date: 26/03/15
 *         Time: 12:15
 */
@Component
public class ExternalItemRecommendationAlgorithm implements ItemRecommendationAlgorithm {

    private static final String URL_PROPERTY_NAME="io.seldon.algorithm.external.url";
    private static final String ALG_NAME_PROPERTY_NAME ="io.seldon.algorithm.external.name";


    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, int dimensionId, int maxRecsCount,
                                                 RecommendationContext ctxt, List<Long> recentItemInteractions) {
        String baseUrl = ctxt.getOptsHolder().getStringOption(URL_PROPERTY_NAME);
        URI uri = URI.create(baseUrl);


        URIBuilder uriBuilder = new URIBuilder().setHost(uri.getHost())
                                                .setPort(uri.getPort())
                                                .setPath(uri.getPath())
                                                .setParameter("client", client)
                                                .setParameter("user_id",user.toString())
                                                .setParameter("item_id",ctxt.getCurrentItem().toString())
                                                .setParameter("limit", String.valueOf(maxRecsCount));
//        HttpGet httpGet = new HttpGet(url).;
//        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
//            CloseableHttpResponse resp = httpclient.execute(httpGet);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return null;
    }

    @Override
    public String name() {
        return null;
    }
}
