/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.importer.articles;

import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

public class FailFast implements Runnable {

	private static Logger logger = Logger.getLogger(FailFast.class.getName());
	
	private final Thread parentThread;
	private boolean keepChecking = true;

	public FailFast(Thread parentThread) {
		this.parentThread = parentThread;
	}
	
	public void run() {
		logger.info("Starting...");
		while (keepChecking) {
			try {
				if (!parentThread.isAlive()) {
					String msg = Thread.currentThread().getName() + ": Killing Process";
					System.out.println(msg);

					{ // Flush the file log appenders
						Enumeration<?> appenders = logger.getAllAppenders();
						while (appenders.hasMoreElements()) {
							Appender  appender = (Appender)appenders.nextElement();
							if (appender instanceof FileAppender) {
								FileAppender fileAppender = (FileAppender)appender;
								fileAppender.setImmediateFlush(true);
							}
						}
					}
					logger.error(msg);
					System.exit(1);
				}
				Thread.sleep(10000); // check every 10 secs
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void stopChecking() {
		keepChecking = false;
	}
}
