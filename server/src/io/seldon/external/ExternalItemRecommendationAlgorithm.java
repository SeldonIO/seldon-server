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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * An Item Recommendation Algorithm that calls out to an HTTP endpoint for its recs.
 *
 * @author firemanphil
 *         Date: 26/03/15
 *         Time: 12:15
 */
@Component
public class ExternalItemRecommendationAlgorithm implements ItemRecommendationAlgorithm {
    private static Logger logger = Logger.getLogger(ExternalItemRecommendationAlgorithm.class.getName());
    private static final String URL_PROPERTY_NAME="io.seldon.algorithm.external.url";
    private static final String ALG_NAME_PROPERTY_NAME ="io.seldon.algorithm.external.name";
    private final PoolingHttpClientConnectionManager cm;
    private final CloseableHttpClient httpClient;
    ObjectMapper mapper = new ObjectMapper();

    private static final int DEFAULT_REQ_TIMEOUT = 200;
    private static final int DEFAULT_CON_TIMEOUT = 500;
    private static final int DEFAULT_SOCKET_TIMEOUT = 2000;
    
    public ExternalItemRecommendationAlgorithm(){
        cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(100);
        cm.setDefaultMaxPerRoute(20);
        
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(DEFAULT_REQ_TIMEOUT)
                .setConnectTimeout(DEFAULT_CON_TIMEOUT)
                .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT).build();
        
        httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }


    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount,
                                                 RecommendationContext ctxt, List<Long> recentItemInteractions) {
        long timeNow = System.currentTimeMillis();
        String recommenderName = ctxt.getOptsHolder().getStringOption(ALG_NAME_PROPERTY_NAME);
        String baseUrl = ctxt.getOptsHolder().getStringOption(URL_PROPERTY_NAME);
        if (ctxt.getInclusionKeys().isEmpty()){
            logger.warn("Cannot get external recommendations are no includers were used. Returning 0 results");
            return new ItemRecommendationResultSet(recommenderName);
        }
        URI uri = URI.create(baseUrl);
        try {
            URIBuilder builder = new URIBuilder().setScheme("http")
                                                    .setHost(uri.getHost())
                                                    .setPort(uri.getPort())
                                                    .setPath(uri.getPath())
                                                    .setParameter("client", client)
                                                    .setParameter("user_id", user.toString())
                                                    .setParameter("recent_interactions",StringUtils.join(recentItemInteractions,","))
                                                    .setParameter("dimensions", StringUtils.join(dimensions, ","))
                                                    .setParameter("exclusion_items", StringUtils.join(ctxt.getExclusionItems(),","))
                                                    .setParameter("data_key", StringUtils.join(ctxt.getInclusionKeys(),","))
                                                    .setParameter("limit", String.valueOf(maxRecsCount));
            if (ctxt.getCurrentItem() != null)
                builder.setParameter("item_id", ctxt.getCurrentItem().toString());
            uri = builder.build();
        } catch (URISyntaxException e) {
            logger.error("Couldn't create URI for external recommender with name " + recommenderName, e);
            return new ItemRecommendationResultSet(recommenderName);
        }
        HttpContext context = HttpClientContext.create();
        HttpGet httpGet = new HttpGet(uri);
        try  {
        	if (logger.isDebugEnabled())
        		logger.debug("Requesting " + httpGet.getURI().toString());
            CloseableHttpResponse resp = httpClient.execute(httpGet, context);
            try
            {
            	if(resp.getStatusLine().getStatusCode() == 200) 
            	{
            		ObjectReader reader = mapper.reader(AlgsResult.class);
            		AlgsResult recs = reader.readValue(resp.getEntity().getContent());
            		List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>(recs.recommended.size());
            		for (AlgResult rec : recs.recommended) {
            			results.add(new ItemRecommendationResultSet.ItemRecommendationResult(rec.item, rec.score));
            		}
            		if (logger.isDebugEnabled())
            			logger.debug("External recommender took "+(System.currentTimeMillis()-timeNow) + "ms");
            		return new ItemRecommendationResultSet(results,recommenderName);
            	} else {
            		logger.error("Couldn't retrieve recommendations from external recommender -- bad http return code: " + resp.getStatusLine().getStatusCode());
            	}
            }
            finally
            {
            	if (resp != null)
					resp.close();
            }
        } catch (IOException e) {
            logger.error("Couldn't retrieve recommendations from external recommender - ", e);
        }
        catch (Exception e)
        {
        	 logger.error("Couldn't retrieve recommendations from external recommender - ", e);
        }
        return new ItemRecommendationResultSet(recommenderName);
    }

    @Override
    public String name() {
        return null;
    }

    public static class AlgsResult {
        public List<AlgResult> recommended;
    }
    public static class AlgResult {
        public Long item;
        public Float score;
    }


}
