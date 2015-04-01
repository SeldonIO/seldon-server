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

package io.seldon.api.controller.vo;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.seldon.api.controller.serialisation.JsonDateSerialiser;
/**
 * Value object for storing the current API version.
 * <p/>
 * Created by: marc on 10/08/2011 at 13:52
 */
public class ApiVersionVo {

    private Integer majorVersion, minorVersion, bugFixVersion;
    private Date versionDate;

    public ApiVersionVo(Integer majorVersion, Integer minorVersion, Integer bugFixVersion, Date versionDate) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.bugFixVersion = bugFixVersion;
        this.versionDate = versionDate;
    }

    @JsonProperty(value = "major")
    public Integer getMajorVersion() {
        return majorVersion;
    }

    @JsonProperty(value = "minor")
    public Integer getMinorVersion() {
        return minorVersion;
    }

    @JsonProperty(value = "fix")
    public Integer getBugFixVersion() {
        return bugFixVersion;
    }

    @JsonProperty(value = "date")
    @JsonSerialize(using = JsonDateSerialiser.class)
    public Date getVersionDate() {
        return versionDate;
    }

    @JsonProperty(value = "version_number")
    public String getVersionNumber() {
        return majorVersion + "." + minorVersion + "." + bugFixVersion;
    }
}
