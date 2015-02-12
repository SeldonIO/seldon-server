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

package io.seldon.api.resource.service;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ConversionBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * @author philipince
 *         Date: 31/01/2014
 *         Time: 11:55
 */
@Component
public class CookieConversionService extends ConversionService {
    @Autowired
    public CookieConversionService(MultiVariateTestStore store) {
        super(store);
    }

    public void submitForConversionCheck(ConsumerBean consumer, UserBean bean, String inviterCookie) {
        if(inviterCookie!=null){
            registerConversion(consumer.getShort_name(), new ConversionBean(ConversionBean.ConversionType.NEW_USER,
                    inviterCookie, bean.getId(), new Date()));
        }
    }
}
