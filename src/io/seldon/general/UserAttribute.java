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
 * Model for <i>declaration</i> of user attributes.
 * <p/>
 * Does not encapsulate specific attribute values.
 * <p/>
 * Created by: marc on 08/08/2011 at 15:40
 */
public class UserAttribute {

    private Integer attributeId;
    private String name;
    private String type;
    private Integer linkType;
    private Boolean demographic;

    public UserAttribute() {
    }

    public UserAttribute(String attributeName, String attributeType) {
        this.name = attributeName;
        this.type = attributeType;
    }

    public UserAttribute(String attributeName, String attributeType, Integer linkType, Boolean demographic) {
        this.name = attributeName;
        this.type = attributeType;
        this.linkType = linkType;
        this.demographic = demographic;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getLinkType() {
        return linkType;
    }

    public void setLinkType(Integer linkType) {
        this.linkType = linkType;
    }

    public Boolean getDemographic() {
        return demographic;
    }

    public void setDemographic(Boolean demographic) {
        this.demographic = demographic;
    }

    @Override
    public String toString() {
        return "UserAttribute{" +
                "attributeId=" + attributeId +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", linkType=" + linkType +
                ", demographic=" + demographic +
                '}';
    }

}
