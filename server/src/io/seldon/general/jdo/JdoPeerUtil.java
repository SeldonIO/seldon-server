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

package io.seldon.general.jdo;

import javax.jdo.JDOHelper;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;

/**
 * Created by: marc on 18/10/2011 at 14:04
 */
public class JdoPeerUtil {

    private static final Logger logger = Logger.getLogger(JdoPeerUtil.class);

    public static <T> void updateRetrievedItem(Class<T> clazz, T candidateObject, T retrievedObject) {
        final Field[] itemFields = clazz.getDeclaredFields();
        for (Field itemField : itemFields) {
            itemField.setAccessible(true);
            try {
                final Object submissionFieldValue = itemField.get(candidateObject);
                if (submissionFieldValue != null) {
                    final String fieldName = itemField.getName();
                    if (fieldName.startsWith("jdo")) {
                        continue;
                    }
                    logger.info("Setting retrieved candidateObject's " + fieldName + " to " + submissionFieldValue);
                    itemField.set(retrievedObject, submissionFieldValue);
                    JDOHelper.makeDirty(retrievedObject, fieldName);
                }
            } catch (IllegalAccessException e) {
                logger.info(e.getMessage());
            }
        }
    }

}
