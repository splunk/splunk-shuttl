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
package com.splunk.shep.archiver.archive.recovery;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.util.UtilsFile;

/**
 * Handling lock of a {@link FileChannel}. Easier to use than {@link FileLock}.
 * </br> Construction method {@link SimpleFileLock#createFromFile(File)} can be
 * used to create the lock.
 */
public class SimpleFileLock {

    private final static Logger logger = Logger.getLogger(SimpleFileLock.class);
    private final FileChannel fileChannel;

    /**
     * Creates a {@link SimpleFileLock} from a file. RuntimeException will be
     * thrown if the file does not exist.
     */
    public static SimpleFileLock createFromFile(File file) {
	UtilsFile.touch(file);
	FileChannel channel = UtilsFile.getFileOutputStreamSilent(file)
		.getChannel();
	return new SimpleFileLock(channel);
    }

    /**
     * @param fileChannel
     *            that is open in write mode to the file to be locked
     */
    public SimpleFileLock(FileChannel fileChannel) {
	this.fileChannel = fileChannel;
    }

    /**
     * @return true if lock was acquired, false otherwise.
     * @throws LockAlreadyClosedException
     *             if this lock was already closed by
     *             {@link SimpleFileLock#closeLock()}
     */
    public boolean tryLock() {
	try {
	    return tryGettingLockOnChannel();
	} catch (OverlappingFileLockException e) {
	    return false; // Expected when locking twice.
	} catch (ClosedChannelException e) {
	    throw new LockAlreadyClosedException("Lock was already closed. "
		    + "Cannot lock this lock that was closed.");
	} catch (IOException e) {
	    logger.debug(did("Tried locking FailedBucketsLock",
		    "Got IOException", "To lock the file", "file_channel",
		    fileChannel, "exception", e));
	    throw new RuntimeException(e);
	}
    }

    private boolean tryGettingLockOnChannel() throws IOException {
	return fileChannel.tryLock() != null;
    }

    /**
     * Releases the lock and closes the channel. Calling
     * {@link SimpleFileLock#tryLock()} after {@link SimpleFileLock#closeLock()}
     * will cause a {@link ClosedChannelException}
     */
    public void closeLock() {
	try {
	    fileChannel.close();
	} catch (IOException e) {
	    // Quietly, without caring about the exception.
	}
    }

    /**
     * Called when {@link SimpleFileLock} was already closed and can't be locked
     * again.
     */
    public class LockAlreadyClosedException extends RuntimeException {
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
}
