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

import java.util.Date;

/**
 * Model for storing version information.
 *
 * Created by: marc on 12/08/2011 at 16:17
 */
public class Version {

    private Integer majorVersion;
    private Integer minorVersion;
    private Integer bugFixVersion;
    private Date releaseDate;

    public Version() {
    }

    public Version(Integer majorVersion, Integer minorVersion, Integer bugFixVersion, Date releaseDate) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.bugFixVersion = bugFixVersion;
        this.releaseDate = releaseDate;
    }

    public Integer getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(Integer majorVersion) {
        this.majorVersion = majorVersion;
    }

    public Integer getMinorVersion() {
        return minorVersion;
    }

    public void setMinorVersion(Integer minorVersion) {
        this.minorVersion = minorVersion;
    }

    public Integer getBugFixVersion() {
        return bugFixVersion;
    }

    public void setBugFixVersion(Integer bugFixVersion) {
        this.bugFixVersion = bugFixVersion;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    @Override
    public String toString() {
        return "Version{" +
                "majorVersion=" + majorVersion +
                ", minorVersion=" + minorVersion +
                ", bugFixVersion=" + bugFixVersion +
                ", releaseDate=" + releaseDate +
                '}';
    }

}
