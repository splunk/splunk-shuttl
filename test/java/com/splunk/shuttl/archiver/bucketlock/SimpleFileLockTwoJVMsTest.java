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
package com.splunk.shuttl.archiver.bucketlock;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.bucketlock.SimpleFileLock;
import com.splunk.shuttl.testutil.ShellClassRunner;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Fixture: Creating SimpleFileLocks in different JVMs.<br/>
 */
@Test(groups = { "slow-unit" })
public class SimpleFileLockTwoJVMsTest {

	static final Integer EXIT_STATUS_ON_FALSE_LOCK = 47;
	static File fileToLock = new File(SimpleFileLockTwoJVMsTest.class.getName()
			+ "-fileToLock");
	SimpleFileLock simpleFileLock;

	@BeforeMethod(groups = { "slow-unit" })
	public void setUp() {
		simpleFileLock = getSimpleFileLock();
	}

	@AfterMethod(groups = { "slow-unit" })
	public void tearDown() {
		deleteFileToLock();
		assertTrue(!fileToLock.exists());
	}

	private void deleteFileToLock() {
		try {
			FileUtils.forceDelete(fileToLock);
		} catch (IOException e) {
			TUtilsTestNG.failForException("Tried force delete on"
					+ " file, got IOException", e);
		}
	}

	@Test(groups = { "slow-unit" })
	public void tryLockExclusive_inOtherJvmAfterLockingInThisJvm_false() {
		assertTrue(simpleFileLock.tryLockExclusive());
		ShellClassRunner otherJvmRunner = new ShellClassRunner();
		otherJvmRunner.runClassAsync(FalseLockInOtherJVM.class);

		assertEquals(EXIT_STATUS_ON_FALSE_LOCK, otherJvmRunner.getExitCode());
	}

	private static SimpleFileLock getSimpleFileLock() {
		return SimpleFileLock.createFromFile(fileToLock);
	}

	private static class FalseLockInOtherJVM {

		// It's launched with the ShellClassRunner.
		@SuppressWarnings("unused")
		public static void main(String[] args) {
			SimpleFileLock simpleFileLock = getSimpleFileLock();
			if (!simpleFileLock.tryLockExclusive())
				System.exit(EXIT_STATUS_ON_FALSE_LOCK);
			else
				System.exit(-1);
		}
	}
}
