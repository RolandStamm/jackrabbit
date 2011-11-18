/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.state;

import javax.transaction.xa.Xid;

import org.apache.jackrabbit.core.TransactionContext;
import org.apache.jackrabbit.core.id.ItemId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Robert.Sauer
 * @version $Revision: $
 */
public class DefaultISMRWLock implements ISMLocking {
    private static final Logger log = LoggerFactory.getLogger(DefaultISMLocking.class);

    private ISMLocking nonXaLocking = new DefaultISMLocking();

    private ISMLocking xaLocking = new XIDAwareISMLocking2();

    public ReadLock acquireReadLock(ItemId id) throws InterruptedException {

	Xid currentXid = TransactionContext.getCurrentXid();

	if (null == currentXid) {
	    return nonXaLocking.acquireReadLock(id);
	} else {
	    return xaLocking.acquireReadLock(id);
	}
    }

    public WriteLock acquireWriteLock(ChangeLog changeLog)
	    throws InterruptedException {
	Xid currentXid = TransactionContext.getCurrentXid();

	if (null == currentXid) {
	    return nonXaLocking.acquireWriteLock(changeLog);
	} else {
	    return xaLocking.acquireWriteLock(changeLog);
	}
    }
}
