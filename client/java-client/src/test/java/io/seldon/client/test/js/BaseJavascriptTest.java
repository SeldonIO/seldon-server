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
package io.seldon.client.test.js;

import io.seldon.client.beans.*;
import io.seldon.client.test.BaseClientTest;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.RandomStringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by: marc on 16/10/2012 at 11:02
 */
public class BaseJavascriptTest extends BaseClientTest {
    @Autowired
    protected JsTestState testState;

    private static final int RANDOM_STRING_LENGTH = 6;
    private HttpClient client;
    private ObjectMapper objectMapper;

    @Before
    public void setup() {
        // Conditionally run this unit:
        Assume.assumeTrue(testState.isEnabled());

        client = new HttpClient();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void createUser() throws IOException {
        String newUser = "/js/user/new";

        NameValuePair[] parameterArray = {
                new NameValuePair("id", testState.getUserPrefix() + randomString()),
                new NameValuePair("consumer_key", testState.getConsumerKey()),
                new NameValuePair("jsonpCallback", testState.getJsonpCallback())
        };

        UserBean userBean = retrievePayload(newUser, parameterArray, UserBean.class);
        System.out.println("Response: " + userBean);
    }

    @Test
    public void createItem() throws IOException {
        String newItem = "/js/item/new";

        NameValuePair[] parameterArray = {
                new NameValuePair("id", testState.getItemPrefix() + randomString()),
                new NameValuePair("type", "1"),
                // ...title, category, tags...
                new NameValuePair("consumer_key", testState.getConsumerKey()),
                new NameValuePair("jsonpCallback", testState.getJsonpCallback())
        };

        ItemBean itemBean = retrievePayload(newItem, parameterArray, ItemBean.class);
        System.out.println("Response: " + itemBean);
    }

    @Test
    public void createAction() throws IOException {
        String newAction = "/js/action/new";

        NameValuePair[] parameterArray = {
                new NameValuePair("user", testState.getUserPrefix() + randomString()),
                new NameValuePair("item", testState.getItemPrefix() + randomString()),
                new NameValuePair("type", "1"),
                new NameValuePair("consumer_key", testState.getConsumerKey()),
                new NameValuePair("jsonpCallback", testState.getJsonpCallback()),
        };

        ActionBean actionBean = retrievePayload(newAction, parameterArray, ActionBean.class);
        System.out.println("Response: " + actionBean);
    }
    
    ///js/recommendations?consumer_key=ruYach9f&user=rand1234&item=http://www.lanazione.it/toscana/cronaca/2012/09/20/774703-buongiorno_toscana.shtml&dimension=1&limit=25&attributes=category,title&jsonpCallback=unused'
    @Test
    public void getRecommendations() throws IOException {
    	
    	String newAction = "/js/action/new";
    	String userId = testState.getUserPrefix() + randomString();
    	
        NameValuePair[] parameterArray = {
                new NameValuePair("user", userId),
                new NameValuePair("item", testState.getItemPrefix() + randomString()),
                new NameValuePair("type", "1"),
                new NameValuePair("consumer_key", testState.getConsumerKey()),
                new NameValuePair("jsonpCallback", testState.getJsonpCallback()),
        };

        ActionBean actionBean = retrievePayload(newAction, parameterArray, ActionBean.class);
        System.out.println("Response: " + actionBean);
    	
        String recommendationPath = "/js/recommendations";

        NameValuePair[] parameterArray2 = {
                new NameValuePair("user", userId),
                new NameValuePair("limit", "25"),
                new NameValuePair("consumer_key", testState.getConsumerKey()),
                new NameValuePair("jsonpCallback", testState.getJsonpCallback()),
        };

        RecommendedItemsBean recBean = retrievePayload(recommendationPath, parameterArray2, RecommendedItemsBean.class);
        System.out.println("Response: " + recBean);
    }

    private <T> T retrievePayload(String path, NameValuePair[] queryParameters, Class<T> valueType) throws IOException {
        GetMethod getMethod = new GetMethod(testState.getEndpoint() + path);
        getMethod.setQueryString(queryParameters);
        String httpReferer = testState.getHttpReferer();
        if ( httpReferer != null ) {
            getMethod.setRequestHeader("Referer", httpReferer);
        }
        client.executeMethod(getMethod);
        String response = getMethod.getResponseBodyAsString();

        // Remove jsonp prefix and suffix:
        String payload = stripJsonp(response);
        logger.info("Payload: " + payload);

        return objectMapper.readValue(payload, valueType);
    }

    private String stripJsonp(String response) {
        return response.replaceAll("^" + testState.getJsonpCallback() + "\\(", "").replaceAll("\\)$", "");
    }

    private String randomString() {
        return randomString(RANDOM_STRING_LENGTH);
    }

    private String randomString(int count) {
        return RandomStringUtils.randomAlphanumeric(count);
    }

}
