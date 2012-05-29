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

import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.recovery.SimpleFileLock.LockAlreadyClosedException;
import com.splunk.shuttl.archiver.archive.recovery.SimpleFileLock.NotLockedException;
import com.splunk.shuttl.testutil.TUtilsFile;

/**
 * Fixture: Abstract. Gets an instance of {@link SimpleFileLock} from
 * {@link SimpleFileLockFromTest#getSimpleFileLock}.
 */
@Test(groups = { "fast-unit" })
public class SimpleFileLockTest {

	SimpleFileLock simpleFileLock;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		simpleFileLock = SimpleFileLock.createFromFile(TUtilsFile.createTestFile());
	}

	@AfterMethod(groups = { "fast-unit" })
	public void tearDown() {
		simpleFileLock.closeLock();
	}

	@Test(groups = { "fast-unit" })
	public void tryLockExclusive_givenOpenFileChannel_trueBecauseItIsNowLocked() {
		assertTrue(simpleFileLock.tryLockExclusive());
		assertTrue(simpleFileLock.isLocked());
		simpleFileLock.closeLock();
		assertTrue(!simpleFileLock.isLocked());
	}

	public void tryLockExclusive_alreadLockedOnce_falseBecauseItCouldntBeLocked() {
		assertTrue(simpleFileLock.tryLockExclusive());
		assertFalse(simpleFileLock.tryLockExclusive());
	}

	@Test(expectedExceptions = { LockAlreadyClosedException.class })
	public void tryLockExclusive_afterLockingAndReleasingOnce_LockAlreadyClosedException() {
		simpleFileLock.tryLockExclusive();
		simpleFileLock.closeLock();
		simpleFileLock.tryLockExclusive();
	}

	@Test(expectedExceptions = { LockAlreadyClosedException.class })
	public void tryLockExclusive_afterClosingLock_LockAlreadyClosedException() {
		simpleFileLock.closeLock();
		simpleFileLock.tryLockExclusive();
	}

	public void closeLock_callingTwice_nothing() {
		simpleFileLock.closeLock();
		simpleFileLock.closeLock();
	}

	public void createFromFile_nonExistingFile_createTheFile() {
		File nonExistingFile = TUtilsFile.createTestFilePath();
		assertTrue(!nonExistingFile.exists());
		SimpleFileLock.createFromFile(nonExistingFile);
		assertTrue(nonExistingFile.exists());
	}

	public void isShared_lockShared_true() {
		simpleFileLock.tryLockShared();
		assertTrue(simpleFileLock.isShared());
	}

	public void isShared_lockExclusive_false() {
		simpleFileLock.tryLockExclusive();
		assertFalse(simpleFileLock.isShared());
	}

	@Test(expectedExceptions = { NotLockedException.class })
	public void isShared_notLocked_throwNotLockedException() {
		assertFalse(simpleFileLock.isLocked());
		simpleFileLock.isShared();
	}

	public void tryConvertExclusiveToSharedLock_afterAcquiringTheLock_lockIsNowShared() {
		assertTrue(simpleFileLock.tryLockExclusive());
		assertFalse(simpleFileLock.isShared());
		assertTrue(simpleFileLock.tryConvertExclusiveToSharedLock());
		assertTrue(simpleFileLock.isShared());
	}

	@Test(expectedExceptions = { NotLockedException.class })
	public void tryConvertExclusiveToSharedLock_withoutHavingLock_throwNotLockedException() {
		assertFalse(simpleFileLock.isLocked());
		simpleFileLock.tryConvertExclusiveToSharedLock();
	}

	public void tryConvertExclusiveToSharedLock_withSharedLock_lockStillShared() {
		assertTrue(simpleFileLock.tryLockShared());
		assertTrue(simpleFileLock.isShared());
		assertTrue(simpleFileLock.tryConvertExclusiveToSharedLock());
		assertTrue(simpleFileLock.isShared());
	}
}
