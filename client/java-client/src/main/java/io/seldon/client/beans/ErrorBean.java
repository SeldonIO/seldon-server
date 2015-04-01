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

import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * @author claudio, marc
 */

@Component
public class ErrorBean extends ResourceBean {

    private static final long serialVersionUID = -8910122705632652975L;
    private int error_id;
    private String error_msg;
    private int http_response;

    private Map<String, String> attributeFailures;

	public ErrorBean() {}

    @SuppressWarnings("unchecked")
    public ErrorBean(Map<String, Object> objectMap) {
        // we could migrate the int fields to Integer -- but this might break current expectations on the client side
        final Integer errorId = (Integer) objectMap.get("error_id");
        final Integer httpResponse = (Integer) objectMap.get("http_response");
        error_id = (errorId == null) ? 0 : errorId;
        error_msg = (String) objectMap.get("error_msg");
        http_response = (httpResponse == null) ? 0 : httpResponse;
        attributeFailures = (Map<String, String>) objectMap.get("attribute_failures");
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

    public Map<String, String> getAttributeFailures() {
        return attributeFailures;
    }

    public void setAttributeFailures(Map<String, String> attributeFailures) {
        this.attributeFailures = attributeFailures;
    }

    @Override
    public String toString() {
        return "ErrorBean{" +
                "error_id=" + error_id +
                ", error_msg='" + error_msg + '\'' +
                ", http_response=" + http_response +
                ", attributeFailures=" + attributeFailures +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ErrorBean)) return false;

        ErrorBean errorBean = (ErrorBean) o;

        if (error_id != errorBean.error_id) return false;
        if (http_response != errorBean.http_response) return false;
        if (attributeFailures != null ? !attributeFailures.equals(errorBean.attributeFailures) : errorBean.attributeFailures != null)
            return false;
        if (error_msg != null ? !error_msg.equals(errorBean.error_msg) : errorBean.error_msg != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = error_id;
        result = 31 * result + (error_msg != null ? error_msg.hashCode() : 0);
        result = 31 * result + http_response;
        result = 31 * result + (attributeFailures != null ? attributeFailures.hashCode() : 0);
        return result;
    }
}
