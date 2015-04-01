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
package io.seldon.importer.articles;

import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;


public class SimpleUrlFetcher implements UrlFetcher {
    
    private static Logger logger = Logger.getLogger(SimpleUrlFetcher.class.getName());

    private final Integer httpGetTimeout;
    
    public SimpleUrlFetcher(Integer httpGetTimeout) {
        this.httpGetTimeout = httpGetTimeout;
    }

    @Override
    public String getUrl(String url) throws Exception {
        String response_content = "";

        HttpClient httpclient = new DefaultHttpClient();
        httpclient.getParams().setParameter(CoreProtocolPNames.USER_AGENT, "Mozilla/5.0 (Macintosh;) SeldonBot/1.0");
        httpclient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, httpGetTimeout);
        httpclient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, httpGetTimeout);
        
        try {
            HttpGet httpget = new HttpGet( url );
            httpget.addHeader("Connection", "close");
            HttpResponse response = httpclient.execute(httpget);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                ContentType contentType = ContentType.getOrDefault(entity);
                logger.info("Response contentType: "+contentType);
                
                String contentCharSet = EntityUtils.getContentCharSet(entity);              
                logger.info("Response contentCharSet: "+contentCharSet);
                
                response_content = EntityUtils.toString(entity);
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            httpclient.getConnectionManager().closeExpiredConnections();
            httpclient.getConnectionManager().closeIdleConnections(30, TimeUnit.SECONDS);
            httpclient.getConnectionManager().shutdown();
        }
        
        return response_content;
    }

}
