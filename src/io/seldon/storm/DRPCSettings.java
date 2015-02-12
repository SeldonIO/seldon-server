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

package io.seldon.storm;

public class DRPCSettings {

	String host;
	int port;
	int timeout;
	String recommendationTopologyName;
	
	
	public DRPCSettings(String host, int port, int timeout,
			String recommendationTopologyName) {
		super();
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.recommendationTopologyName = recommendationTopologyName;
	}
	public String getHost() {
		return host;
	}
	public int getPort() {
		return port;
	}
	public int getTimeout() {
		return timeout;
	}
	public String getRecommendationTopologyName() {
		return recommendationTopologyName;
	}
	
	
}
