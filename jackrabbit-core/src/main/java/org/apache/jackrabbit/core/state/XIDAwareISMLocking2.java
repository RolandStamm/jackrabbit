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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.apache.jackrabbit.core.id.ItemId;
import org.apache.jackrabbit.core.version.IVersioningLock;
import org.apache.jackrabbit.core.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedRef;

/**
 * @author Robert.Sauer
 * @version $Revision: $
 */
public class XIDAwareISMLocking2 implements ISMLocking {
    private static final Logger log = LoggerFactory.getLogger(DefaultISMLocking.class);

    private static final int MODE_IDLE = 0;
    private static final int MODE_READING = 1;
    private static final int MODE_WRITING = 2;

    private final Sync exclusiveness = new ReentrantLock();

    private int currentMode = MODE_IDLE;

    private final SynchronizedRef/* <XidBoundLockInfo> */writer = new SynchronizedRef(
	    null);

    private final Map/* <Xid, XidBoundLockInfo> */readers = new ConcurrentReaderHashMap();

    private LinkedList/* <LockRequest<?>> */intents = new LinkedList();

    /**
     * {@inheritDoc}
     */
    public ReadLock acquireReadLock(ItemId id) throws InterruptedException {

	Xid currentXid = TransactionContext.getCurrentXid();

	try {
	    exclusiveness.acquire();

	    ReadLock readLock = acceptReader(currentXid, intents.isEmpty());

	    if (null != readLock) {
		return readLock;
	    }
	} finally {
	    exclusiveness.release();
	}

	return (ReadLock) enqueueLockRequest(new ReadLockRequest(currentXid));
    }

    /**
     * {@inheritDoc}
     */
    public WriteLock acquireWriteLock(ChangeLog changeLog)
	    throws InterruptedException {

	Xid currentXid = TransactionContext.getCurrentXid();

	try {
	    exclusiveness.acquire();

	    WriteLock writeLock = acceptWriter(currentXid, intents.isEmpty());

	    if (null != writeLock) {
		return writeLock;
	    }
	} finally {
	    exclusiveness.release();
	}

	return (WriteLock) enqueueLockRequest(new WriteLockRequest(currentXid));
    }

    private ReadLock acceptReader(Xid xid, boolean ignoreIntents) {

	if (isCurrentlyWriting(xid)) {
	    // writing implies reading is safe, too
	    XidLockInfo lockInfo = (XidLockInfo) writer.get();
	    if (!readers.containsKey(lockInfo.xid)) {
		readers.put(lockInfo.xid, lockInfo);
	    }
	    return new XidHolderReadLock(lockInfo);

	} else if (isCurrentlyReading(xid)) {
	    // already reading, so keep doing so
	    return new XidHolderReadLock((XidLockInfo) readers.get(xid));

	} else if (ignoreIntents
		&& ((MODE_IDLE == currentMode) || (MODE_READING == currentMode))) {
	    // (optionally) switch into read mode
	    if (MODE_IDLE == currentMode) {
		this.currentMode = MODE_READING;
	    }

	    if (!readers.containsKey(xid)) {
		readers.put(xid, new XidLockInfo(xid));
	    }

	    return new XidHolderReadLock((XidLockInfo) readers.get(xid));

	} else {
	    return null;
	}
    }

    private WriteLock acceptWriter(Xid xid, boolean ignoreIntents) {
	if (isCurrentlyWriting(xid)) {
	    // already writing, so keep doing so
	    return new XidHolderWriteLock((XidLockInfo) writer.get());

	} else if (ignoreIntents && (MODE_IDLE == currentMode)) {
	    // switch into write mode
	    this.currentMode = MODE_WRITING;

	    writer.commit(null, new XidLockInfo(xid));

	    return new XidHolderWriteLock((XidLockInfo) writer.get());

	} else {
	    return null;
	}
    }

    private boolean isCurrentlyWriting(Xid xid) {
	XidLockInfo lockInfo = (XidLockInfo) writer.get();

	return (null != lockInfo) && lockInfo.isSameGlobalTx(xid);
    }

    private boolean isCurrentlyReading(Xid xid) {
	return readers.containsKey(xid);
    }

    private/* <T> T */Object enqueueLockRequest(LockRequest/* <T> */request)
	    throws InterruptedException {
	try {
	    exclusiveness.acquire();

	    // append request to queue
	    intents.add(request);

	    processPendingIntents(request.tsRequested);
	} finally {
	    exclusiveness.release();
	}

	while (null == request.lock) {
	    // wait until this request gets accepted
	    synchronized (request) {
		// periodically wake up to check if someone else forgot to drive
		// progress
		request.wait(1L);
	    }

	    if (null == request.lock) {
		processPendingIntents(request.tsRequested);
	    }
	}

	return request.lock;
    }

    private void processPendingIntents(long tsUntil)
	    throws InterruptedException {
	// try to dispatch as many pending requests as possible
	try {
	    exclusiveness.acquire();

	    if (intents.isEmpty()) {
		return;
	    }

	    LockRequest/* <?> */request = (LockRequest) intents.getFirst();
	    while (request.tsRequested <= tsUntil) {
		if (request.getAccepted(this)) {
		    intents.remove(request);
		    if (Thread.currentThread() == request.owner) {
			// proceed synchronously
			return;
		    } else {
			synchronized (request) {
			    request.notify();
			}

			if (!intents.isEmpty()) {
			    // try next request (e.g. dequeuing multiple readers
			    // in sequence)
			    request = (LockRequest) intents.getFirst();
			} else {
			    // finished processing intents
			    break;
			}
		    }
		} else {
		    // leading request can't be accepted currently
		    break;
		}
	    }
	} finally {
	    exclusiveness.release();
	}
    }

    private class XidHolderReadLock implements ReadLock {
	private final XidLockInfo lockInfo;

	public XidHolderReadLock(XidLockInfo lockInfo) {
	    this.lockInfo = lockInfo;

	    assert lockInfo == readers.get(lockInfo.xid);

	    ++lockInfo.nReadLocks;
	}

	public void release() {
	    try {
		exclusiveness.acquire();

		if (0 >= lockInfo.nReadLocks) {
		    log.warn("Unbalanced release of read lock for XID "
			    + lockInfo.xid, new Throwable());
		} else {
		    if (0 == --lockInfo.nReadLocks) {

			// unregister from readers
			readers.remove(lockInfo.xid);

			// switch from reading to idle ..
			if (readers.isEmpty() && (MODE_READING == currentMode)) {
			    XIDAwareISMLocking2.this.currentMode = MODE_IDLE;
			}

			// .. finally consult queue for pending intents
			processPendingIntents(System.currentTimeMillis());
		    }
		}
	    } catch (InterruptedException ie) {
		log.error("Failed obtaining exclusive lock.", ie);
	    } finally {
		exclusiveness.release();
	    }
	}
    }

    private class XidHolderWriteLock implements WriteLock {
	private final XidLockInfo lockInfo;

	public XidHolderWriteLock(XidLockInfo lockInfo) {
	    this.lockInfo = lockInfo;

	    assert lockInfo == writer.get();

	    ++lockInfo.nWriteLocks;
	}

	/**
	 * {@inheritDoc}
	 */
	public void release() {
	    try {
		exclusiveness.acquire();

		if (writer.get() != lockInfo) {
		    log.warn("Releasing lock from outside active TX.",
			    new Throwable());
		}

		if (0 >= lockInfo.nWriteLocks) {
		    log.warn("Unbalanced release of write lock for XID "
			    + lockInfo.xid, new Throwable());
		} else {
		    if (0 == --lockInfo.nWriteLocks) {
			// unregister from writers
			if (writer.commit(lockInfo, null)) {
			    int targetMode = isCurrentlyReading(lockInfo.xid) ? MODE_READING
				    : MODE_IDLE;

			    assert MODE_WRITING == currentMode;
			    XIDAwareISMLocking2.this.currentMode = targetMode;

			    // .. and consult queue for pending intents
			    processPendingIntents(System.currentTimeMillis());
			} else {
			    log.warn("Unexpected writer detected.");
			}
		    }
		}
	    } catch (InterruptedException ie) {
		log.error("Failed obtaining exclusive lock.", ie);
	    } finally {
		exclusiveness.release();
	    }
	}

	/**
	 * {@inheritDoc}
	 */
	public ReadLock downgrade() {
	    if (!readers.containsKey(lockInfo.xid)) {
		readers.put(lockInfo.xid, lockInfo);
	    }
	    ReadLock readLock = new XidHolderReadLock(lockInfo);

	    release();

	    return readLock;
	}
    }

    private static abstract class LockRequest/* <T> */{
	protected final Xid xid;

	private final long tsRequested = System.currentTimeMillis();

	private final Thread owner = Thread.currentThread();

	Object/* T */lock;

	public LockRequest(Xid xid) {
	    this.xid = xid;
	}

	public abstract boolean getAccepted(XIDAwareISMLocking2 ismLocking);
    }

    private static class ReadLockRequest extends LockRequest/* <ReadLock> */{
	public ReadLockRequest(Xid xid) {
	    super(xid);
	}

	public boolean getAccepted(XIDAwareISMLocking2 ismLocking) {
	    this.lock = ismLocking.acceptReader(xid, true);

	    return (null != lock);
	}
    }

    private static class WriteLockRequest extends LockRequest/* <WriteLock> */{
	public WriteLockRequest(Xid xid) {
	    super(xid);
	}

	public boolean getAccepted(XIDAwareISMLocking2 ismLocking) {
	    this.lock = ismLocking.acceptWriter(xid, true);

	    return (null != lock);
	}
    }

    private static class XidLockInfo {

	private final Xid xid;

	private int nReadLocks = 0;

	private int nWriteLocks = 0;

	public XidLockInfo(Xid xid) {
	    this.xid = xid;
	}

	public Xid getXid() {
	    return xid;
	}

	public boolean isSameGlobalTx(Xid otherXid) {
	    return (xid == otherXid)
		    || Arrays.equals(xid.getGlobalTransactionId(), otherXid
			    .getGlobalTransactionId());
	}

	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((xid == null) ? 0 : xid.hashCode());
	    return result;
	}

	public boolean equals(Object obj) {
	    if (this == obj)
		return true;
	    if (obj == null)
		return false;
	    if (getClass() != obj.getClass())
		return false;
	    XidLockInfo other = (XidLockInfo) obj;
	    if (xid == null) {
		if (other.xid != null)
		    return false;
	    } else if (!xid.equals(other.xid))
		return false;
	    return true;
	}
    }
}
