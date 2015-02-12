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

package io.seldon.facebook.service;

import com.restfb.Facebook;
import com.restfb.Parameter;
import com.restfb.exception.FacebookException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ConversionBean;
import io.seldon.api.resource.service.ConversionService;
import io.seldon.facebook.client.AsyncFacebookClient;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author philipince
 *         Date: 19/12/2013
 *         Time: 13:46
 */
@Component
public class FacebookAppRequestConversionService  extends ConversionService {

    private static final Logger LOGGER = Logger.getLogger(FacebookAppRequestConversionService.class);
    public static final String FQL = "select request_id, recipient_uid, created_time, sender_uid from apprequest " +
            "where recipient_uid = me() " +
            "and strpos(data,'rummble') >= 0 " +
            "and app_id = ";
    private final AsyncFacebookClient fbClient;
    private BlockingQueue<Runnable> tasksToRun = new LinkedBlockingQueue<Runnable>();

    private final ExecutorService executorService;

    @Autowired
    public FacebookAppRequestConversionService(@Value("${facebook.app.request.threads:1}")Integer maxThreads,
                                               AsyncFacebookClient client,
                                               MultiVariateTestStore store){
        super(store);
        this.fbClient = client;
        this.executorService = new ThreadPoolExecutor(1,maxThreads,10, TimeUnit.SECONDS, tasksToRun );
    }

    public void submitForConversionCheck(final String token, final String appId, final int daysToConsider, final ConsumerBean consumer){
        LOGGER.info("Queuing up token "+ token + " for MGM app-request conversion check. Queue size is "+tasksToRun.size()+".");
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("Checking token "+ token + " for MGM conversion.");
                List<FacebookAppRequest> appRequests = null;
                try{
                    appRequests = fbClient.executeFqlQuery(FQL + appId, FacebookAppRequest.class, token);
                    Long validTime = daysToConsider * 24L * 60L * 60L;
                    for(FacebookAppRequest appRequest : appRequests){
                        long timeDist = System.currentTimeMillis()/1000-appRequest.createdTime;
                        if(appRequest.createdTime != null && (timeDist < validTime)){
                            LOGGER.info("Found app request conversion for token "+ token + ", fbid "+appRequest.userId+"");
                            registerConversion(consumer.getShort_name(), new ConversionBean(
                                    ConversionBean.ConversionType.NEW_USER, appRequest.userId, appRequest.senderUid, new Date()));
                            break;
                        }
                        fbClient.deleteObject(appRequest.requestId, Parameter.with("access_token", token));
                    }
                } catch (FacebookException e){
                    LOGGER.error("Facebook error encountered whilst checking for app-request conversion. Continuing... ", e);
                }

            }
        });
    }

    @PreDestroy
    public void shutdown(){
        LOGGER.info("Shutting down...");
        executorService.shutdownNow();
    }

    private static class FacebookAppRequest {

        @Facebook("request_id")
        String requestId;

        @Facebook("recipient_uid")
        String userId;

        @Facebook("created_time")
        Long createdTime;

        @Facebook("sender_uid")
        String senderUid;

    }

}
