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

package io.seldon.general;

/**
 * Model for user attribute-value pairs.
 * <p/>
 * Created by: marc on 09/08/2011 at 13:25
 */
public class UserAttributeValueVo<T> {

    private String name;
    private Integer attributeId;
    private T value;
    private String type;
    private Long userId;

    public UserAttributeValueVo(Integer attributeId, T value) {
        this.attributeId = attributeId;
        this.value = value;
    }

    @SuppressWarnings({"unchecked"})
    public UserAttributeValueVo(Integer attributeId, Object value, String type) {
        this.attributeId = attributeId;
        this.value = (T) value;
        this.type = type;
    }

    public UserAttributeValueVo(Long userId, Integer attributeId, T value, String typeName) {
        this.userId = userId;
        this.attributeId = attributeId;
        this.value = value;
        this.type = typeName;
    }

    public Integer getAttributeId() {
        return attributeId;
    }

    public void setAttributeId(Integer attributeId) {
        this.attributeId = attributeId;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "UserAttributeValueVo{" +
                "name='" + name + '\'' +
                ", attributeId=" + attributeId +
                ", value=" + value +
                ", type='" + type + '\'' +
                ", userId=" + userId +
                '}';
    }
}
