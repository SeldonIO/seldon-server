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

package io.seldon.api.resource.service.business;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.CookieConversionService;
import io.seldon.api.resource.service.MgmInteractionsConversionService;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.service.FacebookAppRequestConversionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author philipince
 *         Date: 25/10/2013
 *         Time: 13:58
 */
@Component
public class ConversionBusinessService {

    private final ConversionCheckType conversionsCheckType;


    private enum ConversionCheckType {
        FACEBOOK("facebook"),
        INTERACTIONS("interactions"),
        COOKIE("cookie"),
        ALL("all");
        public final String type;

        private ConversionCheckType(String type){
            this.type = type;
        }


    }
    FacebookAppRequestConversionService facebookAppRequestConversionService;
    MgmInteractionsConversionService interactionsConversionService;
    CookieConversionService cookieConversionService;
    Integer historyDaysToConsider;

    @Autowired
    public ConversionBusinessService(MgmInteractionsConversionService interactionsConversionService,
                                     FacebookAppRequestConversionService facebookAppRequestConversionService,
                                     CookieConversionService cookieConversionService,
                                     @Value("${conversions.check.type:all}") String conversionsCheckType,
                                     @Value("${conversions.history.days:14}") Integer historyDaysToConsider){
        this.interactionsConversionService = interactionsConversionService;
        this.facebookAppRequestConversionService = facebookAppRequestConversionService;
        this.cookieConversionService = cookieConversionService;
        this.historyDaysToConsider = historyDaysToConsider;
        for(ConversionCheckType checkType : ConversionCheckType.values()){
             if(checkType.type.equals(conversionsCheckType)){
                 this.conversionsCheckType = checkType;
                 return;
             }
         }
        this.conversionsCheckType = ConversionCheckType.ALL;
    }



    public void submitForConversionCheck(ConsumerBean consumer, UserBean bean, String inviterCookie){

        switch (conversionsCheckType){
            case INTERACTIONS:
                interactionsConversionService.submitForConversionCheck(consumer, bean, historyDaysToConsider);
                break;
            case FACEBOOK:
                facebookAppRequestConversionService.submitForConversionCheck(bean.getAttributesName().get(FBConstants.FB_TOKEN),
                        bean.getAttributesName().get(FBConstants.FB_APP_ID),historyDaysToConsider,consumer);
                break;
            case COOKIE:
                cookieConversionService.submitForConversionCheck(consumer, bean, inviterCookie);
                break;
            case ALL:
                if(inviterCookie!=null){
                    cookieConversionService.submitForConversionCheck(consumer, bean, inviterCookie);
                } else {
                    interactionsConversionService.submitForConversionCheck(consumer, bean, historyDaysToConsider);
                }
            default:


        }
    }
}
