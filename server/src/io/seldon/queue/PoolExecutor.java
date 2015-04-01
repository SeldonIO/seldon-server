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

package io.seldon.queue;

import java.util.Observable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.seldon.db.jdo.JDOFactory;
import org.apache.log4j.Logger;

public class PoolExecutor {
    private static Logger logger = Logger.getLogger(PoolExecutor.class);

    private BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>();
    private ThreadPoolExecutor threadPoolExecutor;

    public PoolExecutor(int maxThreads) {
        threadPoolExecutor =
                new ThreadPoolExecutor(maxThreads, maxThreads, 120, TimeUnit.DAYS, queue) {
                    protected void afterExecute(Runnable runnable, Throwable throwable) {
                        if (runnable instanceof Observable) {
                            boolean ok = throwable == null;
                            Observable obs = (Observable) runnable;
                            obs.notifyObservers(ok);
                            if (!ok)
                                logger.error("Uncaught exception afterExecute:", throwable);
                        }
                        try
                        {
                        	JDOFactory.cleanupPM();
                        }
                        catch (Exception e)
                        {
                        	logger.error("Failed to run JDO Cleanup",e);
                        }
                    }
                };
    }

    public int getQSize() {
        return queue.size();
    }

    public void printStats() {
        logger.info(" Waiting:" + queue.size() + " active:" + threadPoolExecutor.getActiveCount());
    }

    public void addJob(Runnable r) {
        threadPoolExecutor.execute(r);
    }

}
