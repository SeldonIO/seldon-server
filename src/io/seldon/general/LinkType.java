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

import io.seldon.api.resource.ResourceBean;

public class LinkType extends ResourceBean {

	/**
	 * is there a reason this shouldn't act like a bean?
	 */
	private Integer typeId;
	private String name;
	private Double defValue;
	
	public LinkType() {}
	
	public LinkType(Integer typeId, String name,Double defValue) {
		super();
		this.typeId = typeId;
		this.name = name;
		this.defValue = defValue;
	}
	
	public Integer getTypeId() {
		return typeId;
	}

	public void setTypeId(Integer typeId) {
		this.typeId = typeId;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public Double getDefValue() {
		return defValue;
	}

	public void setDefValue(Double defValue) {
		this.defValue = defValue;
	}

	@Override
	public String toKey() {
		return typeId.toString();
	}
	
}
