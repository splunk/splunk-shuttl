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
package com.splunk.shep.archiver;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

import org.apache.tools.ant.util.FileUtils;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.ShellClassRunner;

@Test(groups = { "fast" })
public class FileLockTest {

    static File fileToLock = new File(FileLockTest.class.getSimpleName()
	    + "-fileToLock");

    public void fileLock_runningANewJVMTryingToLockTheFile_shouldNotLockTheFile()
	    throws IOException {
	FileLock lock = null;
	try {
	    assertTrue(!fileToLock.exists());
	    assertTrue(fileToLock.createNewFile());
	    assertTrue(fileToLock.exists());

	    lock = tryLockFile(fileToLock);
	    assertNotNull(lock);
	    tryToLockFileFromAnotherJVM();
	} finally {
	    if (lock != null)
		lock.release();
	    FileUtils.delete(fileToLock);
	    assertTrue(!fileToLock.exists());
	}
    }

    private void tryToLockFileFromAnotherJVM() {
	ShellClassRunner shellClassRunner = new ShellClassRunner();
	shellClassRunner.runClassWithArgs(TryToLockFileButCant.class);
	List<String> stdOut = shellClassRunner.getStdOut();
	if (!stdOut.isEmpty()) {
	    System.out.println("Ran class: " + TryToLockFileButCant.class
		    + ", and got output: " + stdOut);
	}
    }

    private static FileLock tryLockFile(File dir) throws FileNotFoundException,
	    IOException {
	FileChannel channel = new FileOutputStream(dir).getChannel();
	return channel.tryLock();
    }

    public static class TryToLockFileButCant {

	public static void main(String[] args) throws FileNotFoundException,
		IOException {
	    assertTrue(fileToLock.exists());
	    FileLock lock = tryLockFile(fileToLock);
	    assertNull(lock);
	}
    }
}
