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
package io.seldon.client.attributes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by: marc on 10/11/2011 at 14:06
 */
abstract class GenericAttributeServiceImpl<T> implements GenericAttributeService<T> {
    
    protected SimpleDateFormat dateFormatter;

    public GenericAttributeServiceImpl() {
        dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    public void addEnumerationAttribute(T obj, String name, String value) {
        Map<String, String> attributesMap = this.ensureAttributesMap(obj);
        attributesMap.put(name, value);
    }

    public void addEnumerationAttributes(T obj, String name, List<String> values) {
        String value = joinString(values);
        addEnumerationAttribute(obj, name, value);
    }

    public void addStringAttribute(T obj, String name, String value) {
        Map<String, String> attributesMap = this.ensureAttributesMap(obj);
        attributesMap.put(name, value);
    }

    public void addStringAttributes(T obj, String name, List<String> values) {
        String value = joinString(values);
        addStringAttribute(obj, name, value);
    }

    public void addDateAttribute(T obj, String name, Date value) {
        Map<String, String> attributesMap = this.ensureAttributesMap(obj);
        attributesMap.put(name, dateFormatter.format(value));
    }

    public void addBooleanAttribute(T obj, String name, Boolean value) {
        Map<String, String> attributesMap = this.ensureAttributesMap(obj);
        String valueString = value ? "1" : "0";
        attributesMap.put(name, valueString);
    }

    public void addNumericalAttribute(T obj, String name, Long value) {
        Map<String, String> attributesMap = this.ensureAttributesMap(obj);
        attributesMap.put(name, value.toString());
    }

    private String joinString(List<String> values) {
        boolean first = true;
        StringBuilder stringBuilder = new StringBuilder();
        for (String value : values) {
            if (!first) {
                stringBuilder.append(",");
            } else {
                first = false;
            }
            stringBuilder.append(value);
        }
        return stringBuilder.toString();
    }

    abstract protected Map<String, String> ensureAttributesMap(T obj);

}
