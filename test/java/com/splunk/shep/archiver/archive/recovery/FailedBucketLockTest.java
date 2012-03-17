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

import java.io.File;

import org.apache.tools.ant.util.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import com.splunk.shep.testutil.UtilsFile;

/**
 * Fixture: new instance of FailedBucketsLock which is a subclass of
 * SimpleFileLock.
 */
public class FailedBucketLockTest extends AbstractSimpleFileLockTest {

    private String originalLockFileName;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.archiver.archive.recovery.AbstractSimpleFileLockTest#
     * getSimpleFileLock()
     */
    @Override
    protected SimpleFileLock getSimpleFileLock() {
	return new FailedBucketLock();
    }

    @BeforeClass
    public void changeLockFileName() {
	originalLockFileName = FailedBucketLock.FAIL_BUCKET_LOCK_FILE_NAME;
	FailedBucketLock.FAIL_BUCKET_LOCK_FILE_NAME = getClass().getName()
		+ ".lock";
    }

    @AfterClass
    public void resetLockFileName() {
	FailedBucketLock.FAIL_BUCKET_LOCK_FILE_NAME = originalLockFileName;
    }

    @AfterMethod(groups = { "fast-unit" })
    public void removeFailedBucketLocksCreatedLockFile() {
	File lockFile = FailedBucketLock.getLockFile();
	FileUtils.delete(lockFile);
	// In the creation of FailedBucketLock, it creates one parent to the
	// lock file. Is this too much knowledge for this test?
	File parentFile = lockFile.getParentFile();
	deleteLockParentIfItsEmpty(parentFile);
    }

    /**
     * In case the parent is empty, delete it. Otherwise it might contain files
     * that shouldn't be deleted.
     */
    private void deleteLockParentIfItsEmpty(File parentFile) {
	if (UtilsFile.isDirectoryEmpty(parentFile)) {
	    FileUtils.delete(parentFile);
	}
    }

}
