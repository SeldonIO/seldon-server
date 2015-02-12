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

package io.seldon.api.resource;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.seldon.api.controller.serialisation.JsonDateSerialiser;
import io.seldon.general.Version;

/**
 * Created by: marc on 12/08/2011 at 16:41
 */
public class VersionBean extends ResourceBean {

    private Integer majorVersion;
    private Integer minorVersion;
    private Integer bugFixVersion;
    private Date releaseDate;

    public VersionBean(Integer majorVersion, Integer minorVersion, Integer bugFixVersion, Date releaseDate) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.bugFixVersion = bugFixVersion;
        this.releaseDate = releaseDate;
    }

    public VersionBean(Version version) {
        majorVersion = version.getMajorVersion();
        minorVersion = version.getMinorVersion();
        bugFixVersion = version.getBugFixVersion();
        releaseDate = version.getReleaseDate();
    }

    @JsonProperty(value = "major")
    public Integer getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(Integer majorVersion) {
        this.majorVersion = majorVersion;
    }

    @JsonProperty(value = "minor")
    public Integer getMinorVersion() {
        return minorVersion;
    }

    public void setMinorVersion(Integer minorVersion) {
        this.minorVersion = minorVersion;
    }

    @JsonProperty(value = "fix")
    public Integer getBugFixVersion() {
        return bugFixVersion;
    }

    public void setBugFixVersion(Integer bugFixVersion) {
        this.bugFixVersion = bugFixVersion;
    }

    @JsonProperty(value = "date")
    @JsonSerialize(using = JsonDateSerialiser.class)
    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    @JsonProperty(value = "version_number")
    public String getVersionNumber() {
        return majorVersion + "." + minorVersion + "." + bugFixVersion;
    }

    @Override
    public String toKey() {
        return majorVersion + "." + minorVersion + "." + bugFixVersion + "." + releaseDate;
    }

    @Override
    public String toString() {
        return "VersionBean{" +
                "majorVersion=" + majorVersion +
                ", minorVersion=" + minorVersion +
                ", bugFixVersion=" + bugFixVersion +
                ", releaseDate=" + releaseDate +
                '}';
    }

}
