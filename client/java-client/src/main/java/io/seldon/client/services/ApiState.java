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
package io.seldon.client.services;

/**
 * Created by: marc on 05/08/2011 at 17:26
 */
public class ApiState {

    private String apiUrl;
    private String secureApiUrl;
    private String consumerKey;
    private String consumerSecret;
    private Boolean preferSecure;
    private Integer readTimeout = 1500; // default of 1.5s (1500ms)

    public String getApiUrl() {
        return apiUrl;
    }

    public void setApiUrl(String apiUrl) {
        this.apiUrl = apiUrl;
    }

    public String getSecureApiUrl() {
        return secureApiUrl;
    }

    public void setSecureApiUrl(String secureApiUrl) {
        this.secureApiUrl = secureApiUrl;
    }

    public String getConsumerKey() {
        return consumerKey;
    }

    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public void setConsumerSecret(String consumerSecret) {
        this.consumerSecret = consumerSecret;
    }

    public Boolean getPreferSecure() {
        return preferSecure;
    }

    public void setPreferSecure(Boolean preferSecure) {
        this.preferSecure = preferSecure;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Integer readTimeout) {
        if ( readTimeout != null ) {
            this.readTimeout = readTimeout;
        }
    }
}
