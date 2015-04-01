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

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.trust.impl.TrustNetworkMember;

public class MemberContentTrustStatePeer {
PersistenceManager pm;
	
	public MemberContentTrustStatePeer(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	public Collection<MemberContentTrustState> getTrustNetwork(long src,int type)
	{
		Query query = pm.newQuery( MemberContentTrustState.class, "srcContent == s && type == t" );
		query.declareParameters( "java.lang.Long s,java.lang.Integer t" );
		Collection<MemberContentTrustState> c = (Collection<MemberContentTrustState>) query.execute( src,type);
		return c;
	}
	
	public MemberContentTrustState get(long src,long dst,int type)
	{
		Query query = pm.newQuery( MemberContentTrustState.class, "srcContent == s && dstContent == d && type == t" );
		query.declareParameters( "java.lang.Long s,java.lang.Long d,java.lang.Integer t" );
		Collection<MemberContentTrustState> c = (Collection<MemberContentTrustState>) query.execute( src,dst,type);
		if (c != null && c.size() == 1)
			return c.iterator().next();
		else
			return null;
	}
	
	public void removeAll()
	{
		Extent<MemberContentTrustState> e = pm.getExtent(MemberContentTrustState.class);
		for(Iterator<MemberContentTrustState> i = e.iterator();i.hasNext();)
		{
			pm.currentTransaction().begin();
			pm.deletePersistent(i.next());
			pm.currentTransaction().commit();
		}
		e.closeAll();
	}
	
	public void saveTrustNetwork(final long src,final TrustNetworkMember tnm)
	{
		try {
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	MemberContentTrustState mts = get(src,tnm.getMember(),tnm.getTrust().type);
			    	if (mts == null)
			    	{
			    		mts = new MemberContentTrustState(src,tnm.getTrust(),tnm.getSixd());
			    		pm.makePersistent(mts);
			    	}
			    	else
			    	{
			    		mts.update(tnm.getTrust(), tnm.getSixd());
			    	}
			    }});
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
