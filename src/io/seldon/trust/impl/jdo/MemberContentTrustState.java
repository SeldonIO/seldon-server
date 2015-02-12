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

package io.seldon.trust.impl.jdo;

import java.util.Date;

import io.seldon.trust.impl.Trust;

public class MemberContentTrustState {

	private long srcContent;
	private int type;
	private long dstContent;
	private double b;
	private double d;
	private double u;
	private double a;
	private int sixd;
	private Date updated;
	
	
	public MemberContentTrustState(long src,Trust t,int sixd)
	{
		this.type = t.type;
		this.srcContent = src == t.m1 ? t.m1 : t.m2;
		this.dstContent = src == t.m1 ? t.m2 : t.m1;
		this.b = t.b;
		this.d = t.d;
		this.u = t.u;
		this.a = src == t.m1 ? t.aM1 : t.aM2;
		this.sixd = sixd;
		this.updated = new Date();
	}

	public void update(Trust t,int sixd)
	{
		this.b = t.b;
		this.d = t.d;
		this.u = t.u;
		this.a = srcContent == t.m1 ? t.aM1 : t.aM2;
		this.sixd = sixd;
		this.updated = new Date();
	}

	public long getSrcContent() {
		return srcContent;
	}

	public int getType() {
		return type;
	}

	public long getDstContent() {
		return dstContent;
	}

	public double getB() {
		return b;
	}

	public double getD() {
		return d;
	}

	public double getU() {
		return u;
	}

	public double getA() {
		return a;
	}

	public int getSixd() {
		return sixd;
	}

	public Date getUpdated() {
		return updated;
	}
	
	
}
