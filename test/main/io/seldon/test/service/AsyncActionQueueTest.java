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

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import io.seldon.api.resource.ActionBean;
import io.seldon.api.service.async.AsyncActionQueue;
import io.seldon.api.service.async.JdoAsyncActionQueue;
import io.seldon.general.Action;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.mahout.common.RandomUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by: marc on 28/10/2011 at 16:53
 */
@Ignore
public class AsyncActionQueueTest extends BaseServiceTest {

    private AsyncActionQueue actionQueue;
    private List<String> ficticiousUsers = new LinkedList<String>();
    private List<String> ficticiousItems = new LinkedList<String>();
    private int userCount = 25;
    private int itemCount = 50;

    @Before
    public void setup() {
        // TODO inject PM
        actionQueue = new JdoAsyncActionQueue(consumerBean.getShort_name(), 5, 25,Integer.MAX_VALUE,3,true,true,true);

        int i;
        for (i = 0; i < userCount; i++) {
            ficticiousUsers.add(RandomStringUtils.randomAlphabetic(12));
        }
        for (i = 0; i < itemCount; i++) {
            ficticiousItems.add(RandomStringUtils.randomAlphabetic(12));
        }
    }

    @Test
    public void simpleActionQueueTest() {
        int actionCount = 1000;
        final Random random = RandomUtils.getRandom();

        for (long i = 0; i < actionCount; ++i) {
            String currentUser = ficticiousUsers.get(random.nextInt(ficticiousUsers.size()));
            String currentItem = ficticiousItems.get(random.nextInt(ficticiousItems.size()));

            logger.info("Action #" + i);
            ActionBean actionBean = new ActionBean(i, currentUser, currentItem, 1, new Date(), 0.0, 1);
            Action action = actionBean.createAction(consumerBean);

            sleepFor(random.nextInt(1000));
            actionQueue.put(action);
        }

    }

    private void sleepFor(int sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ignored) {
        }
    }

}
