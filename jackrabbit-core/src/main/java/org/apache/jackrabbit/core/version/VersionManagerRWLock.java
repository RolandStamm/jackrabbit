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
package org.apache.jackrabbit.core.version;

import javax.transaction.xa.Xid;

import org.apache.jackrabbit.core.TransactionContext;
import org.apache.jackrabbit.core.state.XIDAwareISMLocking;

/**
 * @author Robert.Sauer
 * @version $Revision: $
 */
public class VersionManagerRWLock implements IVersioningLock {

    private IVersioningLock nonXaLocking = new VersioningLock();

    private IVersioningLock xaLocking = new XIDAwareISMLocking();

    public IReadLock acquireReadLock() throws InterruptedException {
	// TODO Auto-generated method stub

	Xid currentXid = TransactionContext.getCurrentXid();

	if (null == currentXid) {
	    return nonXaLocking.acquireReadLock();
	} else {
	    return xaLocking.acquireReadLock();
	}
    }

    public IWriteLock acquireWriteLock()
	    throws InterruptedException {
	Xid currentXid = TransactionContext.getCurrentXid();

	if (null == currentXid) {
	    return nonXaLocking.acquireWriteLock();
	} else {
	    return xaLocking.acquireWriteLock();
	}
    }
}
