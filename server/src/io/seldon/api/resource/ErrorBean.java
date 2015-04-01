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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.seldon.api.APIException;
import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorBean extends ResourceBean {

    private int error_id;
    private String error_msg;
    private int http_response;

    private Map<String, String> attributeFailures;

	public ErrorBean() {}
	
	public ErrorBean(APIException e) {
		error_id = e.getError_id();
		error_msg = e.getError_msg();
		http_response = e.getHttpResponse();
        attributeFailures = e.getFailureMap();
	}
	
	public int getError_id() {
		
		return error_id;
	}
	public void setError_id(int errorId) {
		error_id = errorId;
	}
	public String getError_msg() {
		return error_msg;
	}
	public void setError_msg(String errorMsg) {
		error_msg = errorMsg;
	}
	public int getHttp_response() {
		return http_response;
	}
	public void set_response(int htmlResponse) {
		http_response = htmlResponse;
	}

	public void setHttp_response(int httpResponse) {
		http_response = httpResponse;
	}

	@Override
	public String toKey() {
		return error_id+"";
	}

    @JsonProperty("attribute_failures")
    public Map<String, String> getAttributeFailures() {
        return attributeFailures;
    }

    public void setAttributeFailures(Map<String, String> attributeFailures) {
        this.attributeFailures = attributeFailures;
    }
}
