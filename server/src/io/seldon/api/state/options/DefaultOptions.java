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

package io.seldon.api.state.options;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

/**
 * Store for default options.
 *
 * @author firemanphil
 *         Date: 02/03/15
 *         Time: 17:26
 */
@Component
public class DefaultOptions {
    private Properties props = new Properties();

    @PostConstruct
    public void init() throws IOException {
        InputStream propStream = getClass().getClassLoader().getResourceAsStream("/alg_default.properties");
        props.load(propStream);
        propStream = getClass().getClassLoader().getResourceAsStream("/labs.properties");
        props.load(propStream);
    }

    public String getOption(String optionName){
        return props.getProperty(optionName);
    }



}
