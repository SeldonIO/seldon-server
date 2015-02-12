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
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.UserBean;

/**
 * Created by: marc on 14/08/2012 at 10:56
 */
public interface UserBusinessService {
    ResourceBean updateUser(ConsumerBean consumerBean, UserBean userBean, String inviterCookie,boolean async, boolean fromAppRequest);
}
