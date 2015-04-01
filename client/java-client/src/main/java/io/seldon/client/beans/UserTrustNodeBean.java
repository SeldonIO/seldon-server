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
package io.seldon.client.beans;

import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class UserTrustNodeBean extends ResourceBean {
    private static final long serialVersionUID = 4624650364320075874L;

    private String user;
	private double trust;
	private long pos;

    private Long dimension;
	
	public UserTrustNodeBean() {
	}
	
	public UserTrustNodeBean(String user, double trust,long pos, Long dimension) {
		this.user = user;
		this.trust = trust;
		this.pos = pos;
        this.dimension = dimension;
	}

    public Long getDimension() {
        return dimension;
    }

    public void setDimension(Long dimension) {
        this.dimension = dimension;
    }

    public double getTrust() {
		return trust;
	}

	public void setTrust(double trust) {
		this.trust = trust;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public long getPos() {
		return pos;
	}

	public void setPos(long pos) {
		this.pos = pos;
	}

	
	public int compareTo(UserTrustNodeBean o) {
		if(this.trust == o.trust)
			return this.user.compareTo(o.user);
		else if(this.trust > o.trust)
			return -1;
		else 
			return 1;
	}

    @Override
    public String toString() {
        return "UserTrustNodeBean{" +
                "user='" + user + '\'' +
                ", trust=" + trust +
                ", pos=" + pos +
                ", dimension=" + dimension +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserTrustNodeBean)) return false;

        UserTrustNodeBean that = (UserTrustNodeBean) o;

        if (pos != that.pos) return false;
        if (Double.compare(that.trust, trust) != 0) return false;
        if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) return false;
        if (user != null ? !user.equals(that.user) : that.user != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = user != null ? user.hashCode() : 0;
        temp = trust != +0.0d ? Double.doubleToLongBits(trust) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (pos ^ (pos >>> 32));
        result = 31 * result + (dimension != null ? dimension.hashCode() : 0);
        return result;
    }
}
