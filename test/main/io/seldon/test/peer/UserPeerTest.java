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

package io.seldon.test.peer;

import java.util.Collection;
import java.util.Date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.seldon.general.User;

/**
  * Created by: marc on 05/08/2011 at 12:57
 */
public class UserPeerTest extends BasePeerTest {

    private final static Integer RECENT_USERS = 150;
    private Collection<User> recentUsers;

    @Before
    public void setup() {
        recentUsers = userPeer.getRecentUsers(RECENT_USERS);
    }

    @Test
    public void checkConsumerBean() {
        String shortName = consumerBean.getShort_name();
        System.out.println("Short name: " + shortName);
    }

    @Test
    public void testOperationDates() {
        for (User user : recentUsers) {
            Date firstOp = user.getFirstOp();
            Date lastOp = user.getLastOp();
            boolean valid = firstOp.before(lastOp) || firstOp.equals(lastOp);
            Assert.assertTrue("First operation should predate or equal last operation; " +
                    "first: " + firstOp + " last: " + lastOp, valid);
        }
    }

}
