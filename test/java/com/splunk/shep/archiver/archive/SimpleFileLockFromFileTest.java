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
package com.splunk.shep.archiver.archive;

import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.SimpleFileLock.LockAlreadyClosedException;
import com.splunk.shep.testutil.UtilsFile;

/**
 * Fixture: Creates the SimpleFileLock from the construction method
 * {@link SimpleFileLock#createFromFile(java.io.File)}
 */
@Test(groups = { "fast" })
public class SimpleFileLockFromFileTest {

    SimpleFileLock simpleFileLock;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	simpleFileLock = SimpleFileLock.createFromFile(UtilsFile
		.createTestFile());
    }

    @AfterMethod(groups = { "fast" })
    public void tearDown() {
	simpleFileLock.closeLock();
    }

    public void tryLock_givenOpenFileChannel_trueBecauseItIsNowLocked() {
	assertTrue(simpleFileLock.tryLock());
    }

    public void tryLock_alreadLockedOnce_falseBecauseItCouldntBeLocked() {
	assertTrue(simpleFileLock.tryLock());
	assertFalse(simpleFileLock.tryLock());
    }

    @Test(expectedExceptions = { LockAlreadyClosedException.class })
    public void tryLock_afterLockingAndReleasingOnce_LockAlreadyClosedException() {
	simpleFileLock.tryLock();
	simpleFileLock.closeLock();
	simpleFileLock.tryLock();
    }

    @Test(expectedExceptions = { LockAlreadyClosedException.class })
    public void tryLock_afterClosingLock_LockAlreadyClosedException() {
	simpleFileLock.closeLock();
	simpleFileLock.tryLock();
    }

    public void closeLock_callingTwice_nothing() {
	simpleFileLock.closeLock();
	simpleFileLock.closeLock();
    }

    public void createFromFile_nonExistingFile_createTheFile() {
	File nonExistingFile = UtilsFile.createTestFilePath();
	assertTrue(!nonExistingFile.exists());
	SimpleFileLock.createFromFile(nonExistingFile);
	assertTrue(nonExistingFile.exists());
    }
}
