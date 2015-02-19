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

package io.seldon.test.facebook;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.seldon.api.exception.UserMissingAttributesException;
import io.seldon.api.exception.UserNotFoundException;
import io.seldon.api.resource.UserBean;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoMemoryUserClusterFactory;
import io.seldon.facebook.RummbleFacebookAccount;
import io.seldon.facebook.RummbleFacebookAccountsPeer;
import io.seldon.facebook.exception.FacebookDisabledException;
import io.seldon.facebook.exception.FacebookIdException;
import io.seldon.facebook.exception.FacebookTokenException;
import io.seldon.facebook.importer.FacebookImporterObserver;
import io.seldon.facebook.service.FacebookService;
import io.seldon.semvec.SemanticVectorsStore;
import io.seldon.test.BaseTest;
import io.seldon.test.GenericPropertyHolder;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

/**
 * Created by: marc on 16/12/2011 at 13:48
 */
@ContextConfiguration({"classpath*:/test-service-ctx.xml"})
public class FacebookBaseTest extends BaseTest {

    private static final String CLIENT_USER_ID_PREFIX = "_facebookBaseTest_";

    // This is the only requirement for performing the asynchronous import.
    @Autowired
    private FacebookService facebookService;

    // For monitoring when the import has finished:
    @Autowired
    private FacebookImporterObserver importerObserver;

    // Data-set for testing
    @Autowired
    private RummbleFacebookAccountsPeer facebookAccountsPeer;

    @Autowired
    private GenericPropertyHolder propertyHolder;

    private Collection<RummbleFacebookAccount> accounts;

    @Before
    public void consumerSetup() {
        // Note that this call will only work if we're using a facebook-compatible database; the rest of
        // this unit is database-agnostic.

        accounts = facebookAccountsPeer.getRummbleFacebookAccounts();
        System.out.println(accounts.size());

        Properties clusterProps = new Properties();
        final String consumerName = consumerBean.getShort_name();
        clusterProps.put("io.seldon.memoryuserclusters.clients", consumerName);
		clusterProps.put("io.seldon.memoryuserclusters."+ consumerName +".transient.reloadsecs", "3");
		JdoMemoryUserClusterFactory.initialise(clusterProps);

        // Refactor this:
        Properties properties = new Properties();
        final String semVecBase = propertyHolder.getSemVecBase();
        properties.put("io.seldon.labs.semvec.basedir", semVecBase);
        SemanticVectorsStore.initialise(properties);
    }

    @After 
	public void tearDown()
	{
		MemoryClusterCountFactory.create(new Properties());
		JdoMemoryUserClusterFactory.initialise(new Properties());
	}

    private UserBean facebookUserBean(String facebookId, String facebookToken) {
        UserBean userBean = new UserBean("_fb_" + facebookId);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("facebook", "1");
        attributes.put("facebookId", facebookId);
        attributes.put("facebookToken", facebookToken);
        userBean.setAttributesName(attributes);
        return userBean;
    }

    @Test
    public void asynchronous() throws FacebookTokenException, FacebookDisabledException, UserNotFoundException, FacebookIdException, UserMissingAttributesException {
        final long start = System.currentTimeMillis();
        long userCount = 0;

        for (RummbleFacebookAccount account : accounts) {
            // there are 1,329 rows in my table with null ids; skip over them.
            if (account.getFbId() != null) {
                userCount++;
                String facebookID = account.getFbId().toString();
                String facebookToken = account.getoAuthToken();
                UserBean userBean = facebookUserBean(facebookID, facebookToken);
                facebookService.performAsyncImport(consumerBean, userBean);
            }
        }
        while (importerObserver.isRunning()) {
            try {
                System.out.println("Remaining runners: " + importerObserver.remainingRunners());
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
            }
        }

        final long stop = System.currentTimeMillis();
        System.out.println("Process took: " + (stop - start) + "milliseconds");
        System.out.println("Tried to import: " + userCount + " users.");
    }

    /**
     * Writes to database.
     */
    @Test
    public void asyncJdo() throws FacebookTokenException, FacebookDisabledException, UserNotFoundException, FacebookIdException, UserMissingAttributesException {
        final long start = System.currentTimeMillis();
        long userCount = 0;

        for (RummbleFacebookAccount account : accounts) {
            // there are 1,329 rows in my table with null ids; skip over them.
            if (account.getFbId() != null) {
                userCount++;
                String facebookId = account.getFbId().toString();
                String facebookToken = account.getoAuthToken();
                UserBean userBean = createUserBean();
                final HashMap<String, String> attributes = new HashMap<>();
                userBean.setAttributesName(attributes);
                attributes.put("facebook", "1");
                attributes.put("facebookId", facebookId);
                attributes.put("facebookToken", facebookToken);
                facebookService.performAsyncImport(consumerBean, userBean);
            }
        }
        while (importerObserver.isRunning()) {
            try {
                System.out.println("Remaining runners: " + importerObserver.remainingRunners());
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
            }
        }

        final long stop = System.currentTimeMillis();
        System.out.println("Process took: " + (stop - start) + "milliseconds");
        System.out.println("Tried to import: " + userCount + " users.");
    }

    private UserBean createUserBean() {
        String id = CLIENT_USER_ID_PREFIX + RandomStringUtils.randomAlphabetic(6);
        return new UserBean(id, id);
    }

}
