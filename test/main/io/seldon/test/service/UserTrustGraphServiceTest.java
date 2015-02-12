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

package io.seldon.test.service;

import java.util.List;

import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.UserTrustNodeBean;
import io.seldon.api.resource.service.UserService;
import io.seldon.api.resource.service.UserTrustGraphService;
import org.junit.Test;

/**
 * Created by: marc on 11/08/2011 at 16:07
 */
public class UserTrustGraphServiceTest extends BaseServiceTest {

    public static final int USER_LIMIT = 25;
    public static final int NODE_LIMIT = 25;

    @Test
    public void trustGraph() {
        ListBean usersBean = UserService.getUsers(consumerBean, USER_LIMIT, false);
        List<ResourceBean> users = usersBean.getList();
        for (ResourceBean userResource : users) {
            UserBean userBean = (UserBean) userResource;
            ListBean nodesBean = UserTrustGraphService.getGraph(consumerBean, userBean.getId(), NODE_LIMIT);
            List<ResourceBean> nodes = nodesBean.getList();
            logNodes(nodes);
        }
    }

    private void logNodes(List<ResourceBean> users) {
        for (ResourceBean nodeResource : users) {
            UserTrustNodeBean nodeBean = (UserTrustNodeBean) nodeResource;
            logger.info("User: " + nodeBean.getUser());
        }
    }

}
