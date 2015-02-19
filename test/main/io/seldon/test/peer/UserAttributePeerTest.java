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

import java.sql.Timestamp;
import java.util.*;

import io.seldon.api.Constants;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.general.User;
import io.seldon.general.UserAttribute;
import io.seldon.general.UserAttributeValueVo;
import io.seldon.general.exception.UnknownUserAttributeException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.mahout.common.RandomUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by: marc on 08/08/2011 at 17:55
 */
public class UserAttributePeerTest extends BasePeerTest {

    private final static Logger logger = LoggerFactory.getLogger(UserAttributePeerTest.class);

    // TODO log4j.properties for tests to stdout

    private final static Integer RECENT_USERS = 150;

    @Test
    public void retrieval() {
        Collection<UserAttribute> attributes = userAttributePeer.getAll();
        logger.info("Retrieved " + attributes.size() + " attributes.");
        logAttributes(attributes);
    }

    // TODO add transactional support
    @Test
    public void addAttribute() throws DatabaseException {
        Collection<UserAttribute> attributes = userAttributePeer.getAll();
        int existingAttributeCount = attributes.size();
        UserAttribute attribute = new UserAttribute();
        attribute.setName("[TA]");
        attribute.setType("VARCHAR"); // TODO enum
        logger.info("Attributes before add:");
        logAttributes(attributes);
        userAttributePeer.addAttribute(attribute);
        attributes = userAttributePeer.getAll();
        logger.info("Attributes after add:");
        logAttributes(attributes);
        int countDelta = attributes.size() - existingAttributeCount;
        Assert.assertTrue("There should be an additional attribute, [TA]", (countDelta == 1));
        logger.info("Deleting attribute: " + attribute);
        // TODO this is a hack; integrating service transactions seems tricky
        userAttributePeer.delete(attribute);
    }

    // TODO convert this into an actual test -- for now it's a nice debug method
    @SuppressWarnings("rawtypes")
	@Test
    public void getAttributesForUser() {
        User firstUser = getRecentUser();
        if (firstUser == null) {
            return;
        }
        logger.debug("User: " + firstUser.getUserId());
        Map<Integer, UserAttributeValueVo> attributesForUser = userAttributePeer.getAttributesForUser(firstUser);
        for (Map.Entry<Integer, UserAttributeValueVo> entry : attributesForUser.entrySet()) {
            Integer attributeId = entry.getKey();
            UserAttributeValueVo userAttributeValue = entry.getValue();
            logger.info(attributeId + " -> " + userAttributeValue);
            // TODO establish a safer way of doing this comparison:
            if (userAttributeValue.getType().equals(Constants.TYPE_DATETIME)) {
                // TODO determine whether we want to translate timestamps into Date instances?
                Timestamp timestamp = (Timestamp) userAttributeValue.getValue();
                Long time = timestamp.getTime();
                Date date = new Date(time);
                logger.debug("Date => " + date);
            }
        }
    }

    @Test(expected = UnknownUserAttributeException.class)
    public void unknownAttributeId() throws UnknownUserAttributeException {
        final User recentUser = getRecentUser();
        if ( recentUser == null ) {
            throw new UnknownUserAttributeException("No recent users; trivially true.");
        }

        Collection<UserAttribute> allAttributes = userAttributePeer.getAll();
        Map<Integer, String> knownIds = new HashMap<>();
        for (UserAttribute userAttribute : allAttributes) {
            knownIds.put(userAttribute.getAttributeId(), userAttribute.getName());
        }
        // Pick a random Long not in the map.
        Integer unknownId;
        while (true) {
            unknownId = Math.abs(RandomUtils.getRandom().nextInt());
            if (!knownIds.containsKey(unknownId)) {
                break;
            }
        }
        userAttributePeer.getScalarUserAttributeValueForUser(recentUser, unknownId);
    }

    @Test(expected = UnknownUserAttributeException.class)
    public void unknownAttributeName() throws UnknownUserAttributeException {
        final User recentUser = getRecentUser();
        if ( recentUser == null ) {
            throw new UnknownUserAttributeException("No recent users; trivially true.");
        }

        Collection<UserAttribute> allAttributes = userAttributePeer.getAll();
        Map<String, Integer> knownAttributes = new HashMap<>();
        for (UserAttribute userAttribute : allAttributes) {
            knownAttributes.put(userAttribute.getName(), userAttribute.getAttributeId());
        }
        // Pick a random Long not in the map.
        String unknownName;
        while (true) {
            unknownName = RandomStringUtils.randomAlphabetic(8);
            if (!knownAttributes.containsKey(unknownName)) {
                break;
            }
        }
        userAttributePeer.getScalarUserAttributeValueForUser(getRecentUser(), unknownName);
    }

    @Test
    public void getCompositeAttributeValuesForUser() {
        // (1) IDs
        Map<Integer, Integer> compositeIdMap = userAttributePeer.getCompositeAttributesForUser(1L);
        for (Map.Entry<Integer, Integer> entry : compositeIdMap.entrySet()) {
            logger.info(entry.getKey() + " -> " + entry.getValue());
        }
        // (2) Names
        Map<String, String> compositeNameMap = userAttributePeer.getCompositeAttributeNamesForUser(1L);
        for (Map.Entry<String, String> entry : compositeNameMap.entrySet()) {
            logger.info(entry.getKey() + " -> " + entry.getValue());
        }
    }

    private User getRecentUser() {
        Collection<User> recentUsers = userPeer.getRecentUsers(RECENT_USERS);
        Iterator<User> userIterator = recentUsers.iterator();
        if (!userIterator.hasNext()) {
            return null;
        }
        return userIterator.next();
    }

    private void logAttributes(Collection<UserAttribute> attributes) {
        for (UserAttribute attribute : attributes) {
            logAttribute(attribute);
        }
    }

    private void logAttribute(UserAttribute attribute) {
        logger.info(attribute.toString());
    }

}
