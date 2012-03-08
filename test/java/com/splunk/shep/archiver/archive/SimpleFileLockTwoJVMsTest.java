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
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.ShellClassRunner;

/**
 * Fixture: Creating SimpleFileLocks in different JVMs.<br/>
 * Variables are all static so that they are created when
 * {@link FalseLockInOtherJVM#main(String[])} is running.
 */
@Test(groups = { "slow" })
public class SimpleFileLockTwoJVMsTest {

    static final Integer EXIT_STATUS_ON_FALSE_LOCK = 47;
    static File fileToLock = new File(SimpleFileLockTwoJVMsTest.class.getName()
	    + "-fileToLock");
    static SimpleFileLock simpleFileLock = SimpleFileLock
	    .createFromFile(fileToLock);

    @AfterMethod
    public void tearDown() {
	try {
	    FileUtils.forceDelete(fileToLock);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
	assertTrue(!fileToLock.exists());
    }

    public void tryLock_inOtherJvmAfterLockingInThisJvm_false() {
	assertTrue(simpleFileLock.tryLock());
	ShellClassRunner otherJvmRunner = new ShellClassRunner();
	otherJvmRunner.runClassWithArgs(FalseLockInOtherJVM.class);

	assertEquals(EXIT_STATUS_ON_FALSE_LOCK, otherJvmRunner.getExitCode());
    }

    private static class FalseLockInOtherJVM {

	// It's launched with the ShellClassRunner.
	@SuppressWarnings("unused")
	public static void main(String[] args) {
	    assertFalse(simpleFileLock.tryLock());
	    System.exit(EXIT_STATUS_ON_FALSE_LOCK);
	}
    }
}
