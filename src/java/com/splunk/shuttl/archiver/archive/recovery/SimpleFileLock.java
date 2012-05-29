// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.archiver.archive.recovery;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.util.UtilsFile;

/**
 * Handling lock of a {@link FileChannel}. Easier to use than {@link FileLock}.
 * </br> Construction method {@link SimpleFileLock#createFromFile(File)} can be
 * used to create the lock.
 */
public class SimpleFileLock {

	private final static Logger logger = Logger.getLogger(SimpleFileLock.class);
	private final FileChannel fileChannel;
	private FileLock fileLock;

	/**
	 * Creates a {@link SimpleFileLock} from a file. RuntimeException will be
	 * thrown if the file does not exist.
	 */
	public static SimpleFileLock createFromFile(File file) {
		UtilsFile.touch(file);
		FileChannel channel = UtilsFile.getRandomAccessFileSilent(file)
				.getChannel();
		return new SimpleFileLock(channel);
	}

	/**
	 * @param fileChannel
	 *          that is open in write mode to the file to be locked
	 */
	public SimpleFileLock(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	/**
	 * @return true if lock was acquired, false otherwise.
	 * @throws LockAlreadyClosedException
	 *           if this lock was already closed by
	 *           {@link SimpleFileLock#closeLock()}
	 */
	public boolean tryLockExclusive() {
		boolean shared = false;
		return tryGettingLockOnChannel(shared);
	}

	private boolean tryGettingLockOnChannel(boolean shared) {
		fileLock = getLockWithErrorHandling(shared);
		return isLocked();
	}

	private FileLock getLockWithErrorHandling(boolean shared) {
		try {
			return fileChannel.tryLock(0, Long.MAX_VALUE, shared);
		} catch (OverlappingFileLockException e) {
			return null; // Expected when locking twice.
		} catch (ClosedChannelException e) {
			throw new LockAlreadyClosedException("Lock was already closed. "
					+ "Cannot lock this lock that was closed.");
		} catch (IOException e) {
			logger.debug(did("Tried locking FailedBucketsLock", "Got IOException",
					"To lock the file", "file_channel", fileChannel, "exception", e));
			throw new RuntimeException(e);
		}
	}

	/**
	 * Tries to get lock of a file with a shared lock. Which means that it can
	 * lock a file, even though it's already locked.
	 * 
	 * @return true if lock was acquired.
	 */
	public boolean tryLockShared() {
		boolean shared = true;
		return tryGettingLockOnChannel(shared);
	}

	/**
	 * Releases the lock and closes the channel. Calling
	 * {@link SimpleFileLock#tryLockExclusive()} after
	 * {@link SimpleFileLock#closeLock()} will cause a
	 * {@link ClosedChannelException}
	 */
	public void closeLock() {
		IOUtils.closeQuietly(fileChannel);
	}

	/**
	 * @return true if is locked and shared
	 * @throws {@link NotLockedException} if is not locked.
	 */
	public boolean isShared() {
		if (!isLocked())
			throw new NotLockedException();
		else
			return fileLock.isShared();
	}

	/**
	 * Converts an exclusive lock to a shared lock.<br/>
	 * Note: It might fail because it temporarily releases its lock and then tries
	 * to regain it. Some other JVM might take control between this release and
	 * regain.
	 * 
	 * @return true if convert from exclusive to shared was successful.
	 * @throws {@link NotLockedException} if is not locked.
	 */
	public boolean tryConvertExclusiveToSharedLock() {
		if (!isLocked())
			throw new NotLockedException();
		else if (isShared())
			return true;
		else
			return releaseLockAndTryGettingSharedLock();
	}

	private boolean releaseLockAndTryGettingSharedLock() {
		releaseLock();
		return tryLockShared();
	}

	private void releaseLock() {
		try {
			fileLock.release();
		} catch (IOException e) {
			logger.warn(warn("Released a lock on a file.", e,
					"Will not do anything about it", "file_channel", fileChannel));
		}
	}

	/**
	 * @return true if is has acquired lock.
	 */
	public boolean isLocked() {
		return fileLock != null && fileLock.isValid();
	}

	/**
	 * Called when {@link SimpleFileLock} was already closed and can't be locked
	 * again.
	 */
	public static class LockAlreadyClosedException extends RuntimeException {
		/**
		 * Default generated serial version uid.
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * Only used to call super implementation.
		 */
		public LockAlreadyClosedException(String string) {
			super(string);
		}
	}

	public static class NotLockedException extends RuntimeException {
		/**
		 * Default generated serial version uid.
		 */
		private static final long serialVersionUID = 1L;

		public NotLockedException(String string) {
			super(string);
		}

		public NotLockedException() {
		}

	}

}
