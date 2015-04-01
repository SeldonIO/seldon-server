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

/*
 * Created on 24-May-2006
 *
 */
package io.seldon.db.jdo;

import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;

public class JDOLifecycleListener extends ThreadLocal implements DirtyLifecycleListener{
    //private static Logger logger = Logger.getLogger( JDOLifecycleListener.class.getName() );

    
    public void preDirty(InstanceLifecycleEvent arg0)
    {
    }
    public void postDirty(javax.jdo.listener.InstanceLifecycleEvent event)
    {
        final String message = "Post dirty event for " + event.getSource().getClass().getName();
        //logger.error(message, new Exception(message));
        if (get() == null)
            set(new Boolean(true));
    }
    
    public boolean isDirty()
    {
        return (get() != null);
    }
    
    public void clear() { set(null); }
}
