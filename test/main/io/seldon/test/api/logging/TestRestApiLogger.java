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

package io.seldon.test.api.logging;

import static org.easymock.EasyMock.*;

import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.resource.ConsumerBean;
import org.junit.Test;

import io.seldon.api.logging.ApiLogger;

public class TestRestApiLogger {

	@Test
	public void testLog()
	{
		HttpServletRequest req = createMock(HttpServletRequest.class);
		expect(req.getQueryString()).andReturn("attributes=value1,value2,value3");
		expect(req.getMethod()).andReturn("POST").anyTimes();
		expect(req.getServerName()).andReturn("SERVER").anyTimes();
		expect(req.getContextPath()).andReturn("/path").anyTimes();
		expect(req.getServletPath()).andReturn("/servlet").anyTimes();
		replay(req);
		ApiLogger.log("TEST", new Date(), req, new ConsumerBean("TEST"));
	}
	
	@Test
	public void testLogWithNullQueryString()
	{
		HttpServletRequest req = createMock(HttpServletRequest.class);
		expect(req.getQueryString()).andReturn(null);
		expect(req.getMethod()).andReturn("POST").anyTimes();
		expect(req.getServerName()).andReturn("SERVER").anyTimes();
		expect(req.getContextPath()).andReturn("/path").anyTimes();
		expect(req.getServletPath()).andReturn("/servlet").anyTimes();
		replay(req);
		ApiLogger.log("TEST", new Date(), req, new ConsumerBean("TEST"));
	}

}
