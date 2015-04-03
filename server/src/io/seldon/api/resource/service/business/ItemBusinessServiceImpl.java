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

import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.service.ApiLoggerServer;

import org.springframework.beans.factory.annotation.Autowired;

public class ItemBusinessServiceImpl implements ItemBusinessService {

	 @Autowired
	 private ItemService itemService;
	
    @Override
    public ResourceBean updateItem(ConsumerBean consumerBean, ItemBean itemBean) {
        ResourceBean responseBean;
        try {
            itemService.updateItem((ConsumerBean) consumerBean, itemBean);
            responseBean = itemBean;
        } catch (APIException e) {
            ApiLoggerServer.log(this, e);
            responseBean = new ErrorBean(e);
        } catch (Exception e) {
            ApiLoggerServer.log(this, e);
            APIException apiEx = new APIException(APIException.INCORRECT_FIELD);
            responseBean = new ErrorBean(apiEx);
        }
        return responseBean;
    }

}