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

package io.seldon.clustering.recommender;

import java.io.Serializable;

public class UserCluster implements Serializable, Comparable<UserCluster> {
	long user;
	int cluster;
	double weight;
	long timeStamp;
	int group;
	public UserCluster(long user, int cluster, double weight,long timestamp,int group) {
		super();
		this.user = user;
		this.cluster = cluster;
		this.weight = weight;
		this.timeStamp = timestamp;
		this.group = group;
	}
	public String toString()
	{
		return "UserCluster user:"+user+" cluster:"+cluster+" weight:"+weight+" time:"+timeStamp;
	}
	
	public long getUser() {
		return user;
	}
	public int getCluster() {
		return cluster;
	}
	public double getWeight() {
		return weight;
	}
	public void setUser(long user) {
		this.user = user;
	}
	public void setCluster(int cluster) {
		this.cluster = cluster;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public int getGroup() {
		return group;
	}
	public void setGroup(int group) {
		this.group = group;
	}
	
	public void merge(UserCluster o)
	{
		if (o.cluster == cluster && o.user == user && o.timeStamp == timeStamp)
		{
			weight = (weight + o.weight)/2.0D;
		}
	}
	@Override
	public int compareTo(UserCluster o) {
		if (weight < o.weight)
			return -1;
		else if (weight > o.weight)
			return 1;
		else
			return 0;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof UserCluster))
			return false;
		else
		{
			UserCluster oc = (UserCluster) o;
			if (oc.user == user && oc.cluster == cluster && oc.timeStamp == timeStamp)
				return true;
			else
				return false;
		}
	}
	
}
