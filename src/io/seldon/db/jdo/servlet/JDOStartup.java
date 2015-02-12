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

package io.seldon.db.jdo.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;

import io.seldon.db.jdo.JDOFactory;

public class JDOStartup {
	
	public static void contextInitialized(ServletContextEvent sce) throws NamingException
	{
		Properties dbprops = new Properties();
    	Properties dbfactories = new Properties();
//    	InputStream propStream = sce.getServletContext().getResourceAsStream("/WEB-INF/datanucleus.properties");
//    	InputStream propStreamFactories = sce.getServletContext().getResourceAsStream("/WEB-INF/jdofactories.properties");
        final ClassLoader classLoader = JDOStartup.class.getClassLoader();
        InputStream propStream = classLoader.getResourceAsStream("/datanucleus.properties");
    	InputStream propStreamFactories = classLoader.getResourceAsStream("/jdofactories.properties");
    	try
    	{
    		dbprops.load( propStream );
    		dbfactories.load( propStreamFactories);
    		JDOFactory.initialise(dbprops, dbfactories);
    	}
    	catch( IOException ex )
    	{
             
    	}
	}

}
