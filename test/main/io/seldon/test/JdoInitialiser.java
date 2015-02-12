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

package io.seldon.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.naming.NamingException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.mock.jndi.SimpleNamingContextBuilder;

import io.seldon.api.Constants;
import io.seldon.db.jdo.JDOFactory;

/**
 * Helper class for bridging Spring test context with existing project structure
 * (in particular, factory methods and property loading).
 * <p/>
 * Mostly a copy of {@link io.seldon.db.jdo.servlet.JDOStartup#contextInitialized(javax.servlet.ServletContextEvent)}
 * <p/>
 * Created by: marc on 05/08/2011 at 13:39
 */
public class JdoInitialiser {

    //private final static Logger logger = LoggerFactory.getLogger(JdoInitialiser.class);

    @Autowired
    private DriverManagerDataSource dataSource;

    private String consumerName;
    private String dataNucleusPropertiesLocation;
    private String jndiResourceName;

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public void setDataNucleusPropertiesLocation(String dataNucleusPropertiesLocation) {
        this.dataNucleusPropertiesLocation = dataNucleusPropertiesLocation;
    }

    public void setJndiResourceName(String jndiResourceName) {
        this.jndiResourceName = jndiResourceName;
    }

    public Boolean initialiseJdoFactory() throws NamingException, IOException {
        setupMockEnvironment(jndiResourceName, dataSource);

        InputStream dnStream = JdoInitialiser.class.getResourceAsStream(dataNucleusPropertiesLocation);
        Properties dnProperties = new Properties();

        dnProperties.load(dnStream);

        JDOFactory.initialise(dnProperties, consumerName, null, jndiResourceName);
        JDOFactory.initialise(dnProperties, Constants.API_DB, null, jndiResourceName);
        return true;
    }

    /**
     * Bind the data source to the supplied JNDI resource.
     *
     * @param jndiResourceName the resource to bind to
     * @param dataSource data source (should be set up in Spring context)
     * @throws NamingException if binding activation fails
     */
    private void setupMockEnvironment(String jndiResourceName, DriverManagerDataSource dataSource) throws NamingException {
        SimpleNamingContextBuilder builder = new SimpleNamingContextBuilder();
        builder.bind(jndiResourceName, dataSource);
        builder.activate();
    }

}
