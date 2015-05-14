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
package io.seldon.external;

import io.seldon.api.APIException;
import io.seldon.prediction.PredictionResult;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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

@Component
public class ExternalPredictionServer {
	private static Logger logger = Logger.getLogger(ExternalPredictionServer.class.getName());
    private static final String URL_PROPERTY_NAME="io.seldon.algorithm.external.url";
    private static final String ALG_NAME_PROPERTY_NAME ="io.seldon.algorithm.external.name";
    private final PoolingHttpClientConnectionManager cm;
    private final CloseableHttpClient httpClient;
    ObjectMapper mapper = new ObjectMapper();

    public ExternalPredictionServer(){
        cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(100);
        cm.setDefaultMaxPerRoute(20);
        httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
    }
    
    public PredictionResult predict(String client, String json) 
    {
    		long timeNow = System.currentTimeMillis();
    		URI uri = URI.create("http://localhost:1234");
    		try {
    			URIBuilder builder = new URIBuilder().setScheme("http")
    					.setHost(uri.getHost())
    					.setPort(uri.getPort())
    					.setPath(uri.getPath())
    					.setParameter("client", client)
    					.setParameter("json", json);

    			uri = builder.build();
    		} catch (URISyntaxException e) 
    		{
    			throw new APIException(APIException.GENERIC_ERROR);
    		}
    		HttpContext context = HttpClientContext.create();
    		HttpGet httpGet = new HttpGet(uri);
    		try  
    		{
    			logger.debug("Requesting " + httpGet.getURI().toString());
    			CloseableHttpResponse resp = httpClient.execute(httpGet, context);
    			if(resp.getStatusLine().getStatusCode() == 200) 
    			{
    				ObjectReader reader = mapper.reader(PredictionResult.class);
    				PredictionResult res = reader.readValue(resp.getEntity().getContent());
    				logger.debug("External prediction server took "+(System.currentTimeMillis()-timeNow) + "ms");
    				return res;
    			} 
    			else 
    			{
    				logger.error("Couldn't retrieve prediction from external prediction server -- bad http return code: " + resp.getStatusLine().getStatusCode());
    				throw new APIException(APIException.GENERIC_ERROR);
    			}
    		} 
    		catch (IOException e) 
    		{
    			logger.error("Couldn't retrieve prediction from external prediction server - ", e);
    			throw new APIException(APIException.GENERIC_ERROR);
    		}

    }

    
}
