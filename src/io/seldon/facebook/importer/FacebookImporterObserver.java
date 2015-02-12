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

package io.seldon.facebook.importer;

import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.*;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.UserBean;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.facebook.FacebookUserGraph;
import org.apache.log4j.Logger;

/**
 * Created by: marc on 16/08/2011 at 17:20
 */
abstract public class FacebookImporterObserver implements Observer {

    //private ExecutorService executor = Executors.newFixedThreadPool(25);

    private int runners = 0;
    private static final Logger logger = Logger.getLogger(FacebookImporterObserver.class);

        public static final int CORE_POOL_SIZE = 1;
    public static final int MAX_POOL_SIZE = 75;

    private BlockingQueue<Runnable> queue = new LinkedBlockingDeque<Runnable>();
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, 120, TimeUnit.DAYS, queue) {
        protected void afterExecute(java.lang.Runnable runnable, java.lang.Throwable throwable) {
            logger.info("Cleaning up Persistence Manager for thread.");
            FacebookImporter facebookImporter = null;
            if (runnable instanceof FacebookImporter) {
                facebookImporter = (FacebookImporter) runnable;
            }
            if (throwable != null) {
                if (throwable instanceof JDOException) {
                    if ( facebookImporter != null ) {
                        // it shouldn't be since we got a JDO exception
                        final ConsumerBean consumerBean = facebookImporter.getConsumerBean();
                        PersistenceManager persistenceManager = JDOFactory.getPersistenceManager(consumerBean.getShort_name());
                    TransactionPeer.rollbackIfActive(persistenceManager);
                    update((Observable) runnable, null);
                    logger.warn("Caught a JDOException: " + throwable.getMessage());
                    }
                } else {
                    logger.warn("Unexpected exception: " + throwable.getMessage());
                }
                // Try to deal with DataNucleus problem (see LABS-385) -- sorry, a little ugly.
                /*
                if (facebookImporter != null) {
                    FacebookImporterRunner runner = (FacebookImporterRunner) runnable;
                    FacebookImporterRunner replacementRunner;
                    if (runner.isFirstAttempt()) {
                        logger.warn("Creating a replacement runner for: " + runner);
                        replacementRunner = runner.getNewInstance();
                        replacementRunner.setFirstAttempt(false);
                        runners++;
                        threadPoolExecutor.execute(replacementRunner);
                    } else {
                        logger.warn("*NOT* creating another replacement runner for: " + runner + ".");
                    }
                }
                */
            }

            runners--;
            JDOFactory.cleanupPM();
            logger.info("afterExecute: Remaining runners: " + runners);
        }
    };

    public void addRunner(FacebookImporter runner) {
        runner.addObserver(this);
        runners++;
        executor.execute(runner);
        //threadPoolExecutor.execute(runner);
    }

    public boolean isRunning() {
        return runners > 0;
    }

    public int remainingRunners() {
        return runners;
    }

    /**
     * An observable instance should notify us when it's finished importing.
     *
     * @param observable a runner
     * @param o          success?
     */
    public void update(Observable observable, Object o) {
        if (observable instanceof FacebookImporter) {
            FacebookUserGraph userGraph = (FacebookUserGraph) o;
            FacebookImporter runner = (FacebookImporter) observable;
            if ( userGraph == null ) {
                // afterExecute(...) will call update with a null userGraph
                logger.warn("Runner terminated abnormally: " + runner + "; graph: " + userGraph);
            } else {
                logger.info("Runner finished: " + runner + "; graph: " + userGraph);
                logger.info("-> runners: " + runners);
                processGraph(runner.getUserBean(), runner.getConsumerBean(), runner.getFacebookId(), userGraph);
            }
        }
    }

    abstract protected void processGraph(UserBean userBean, ConsumerBean consumerBean, String facebookId, FacebookUserGraph userGraph);

}
