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
package io.seldon.client.test;

import io.seldon.client.beans.ErrorBean;
import io.seldon.client.beans.ResourceBean;
import io.seldon.client.services.ApiService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Created by: marc on 05/08/2011 at 17:41
 */
@ContextConfiguration({"classpath*:api-beans-ctx.xml"})
abstract public class BaseBeansTest extends AbstractJUnit4SpringContextTests {

    @Autowired
    protected ApiService apiService;

    /**
     * If the supplied resourceBean is an instance of {@link ErrorBean}, throw an exception.
     *
     * @param resourceBean the {@link ResourceBean} to check
     * @return the same resourceBean
     * @throws ErrorBeanException if resourceBean is an instance of {@link ErrorBean}
     */
    protected ResourceBean checkedResource(ResourceBean resourceBean) throws ErrorBeanException {
        if (resourceBean instanceof ErrorBean) {
            throw new ErrorBeanException(resourceBean);
        }
        return resourceBean;
    }

}
