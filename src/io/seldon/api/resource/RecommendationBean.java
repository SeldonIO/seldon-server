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

import java.util.List;
import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class RecommendationBean extends ResourceBean {

	String item;
	long pos;
	List<UserTrustNodeBean> srcUsers;
	
	public RecommendationBean() {}
	
	

	public RecommendationBean(String item, long pos, List<UserTrustNodeBean> srcUsers) {
		this.item = item;
		this.pos = pos;
		this.srcUsers = srcUsers;
	}



	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public long getPos() {
		return pos;
	}

	public void setPos(long pos) {
		this.pos = pos;
	}

	public List<UserTrustNodeBean> getSrcUsers() {
		return srcUsers;
	}

	public void setSrcUsers(List<UserTrustNodeBean> srcUsers) {
		this.srcUsers = srcUsers;
	}

	@Override
	public String toKey() {
		return item;
	}
	
}
