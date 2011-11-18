package org.apache.jackrabbit.core.version;

public interface IVersioningLock {

    public IWriteLock acquireWriteLock() throws InterruptedException;

    public IReadLock acquireReadLock() throws InterruptedException;

    public interface IWriteLock {

	public abstract IReadLock downgrade() throws InterruptedException;

	public abstract void release();

    }

    public interface IReadLock {

	public abstract void release();

    }
}
