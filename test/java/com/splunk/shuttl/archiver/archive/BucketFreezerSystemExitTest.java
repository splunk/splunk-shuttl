// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.recovery.BucketMover;
import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.bucketlock.BucketLockerInTestDir;
import com.splunk.shuttl.server.mbeans.util.RegistersMBeans;
import com.splunk.shuttl.testutil.TUtilsFile;

/**
 * Fixture: Calling the main method of BucketFreezer. Testing exit codes.
 */
@Test(groups = { "fast-unit" })
public class BucketFreezerSystemExitTest {

	Runtime runtimeMock;
	BucketFreezer bucketFreezer;
	BucketFreezerProvider bucketFreezerProvider;
	File testDir;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		runtimeMock = mock(Runtime.class);
		testDir = createDirectory();
		bucketFreezer = new BucketFreezer(new BucketMover(testDir),
				new BucketLockerInTestDir(testDir), mock(ArchiveRestHandler.class),
				mock(FailedBucketsArchiver.class));
		bucketFreezerProvider = mock(BucketFreezerProvider.class);
		stub(bucketFreezerProvider.getConfiguredBucketFreezer()).toReturn(
				bucketFreezer);
	}

	@AfterMethod(groups = { "fast-unit" })
	public void tearDown() throws IOException {
		FileUtils.deleteDirectory(testDir);
	}

	@Test(groups = { "fast-unit" })
	public void main_existingDirecotry_returnCode0() throws IOException {
		File directory = TUtilsFile.createDirectory();
		runMainWithDepentencies_withArguments("index-name",
				directory.getAbsolutePath());
		verify(runtimeMock).exit(0);
	}

	public void main_noArguments_exitWithIncorrectArgumentsConstant() {
		runMainWithDepentencies_withArguments();
		verify(runtimeMock).exit(BucketFreezer.EXIT_INCORRECT_ARGUMENTS);
	}

	public void main_oneArgument_exitWithIncorrectArgumentsConstant() {
		runMainWithDepentencies_withArguments("index-name");
		verify(runtimeMock).exit(BucketFreezer.EXIT_INCORRECT_ARGUMENTS);
	}

	public void main_threeArguments_exitWithIncorrectArgumentsConstant() {
		runMainWithDepentencies_withArguments("index-name", "/path/to/file",
				"too-many-arguments");
		verify(runtimeMock).exit(BucketFreezer.EXIT_INCORRECT_ARGUMENTS);
	}

	public void main_tooManyArguments_exitWithIncorrectArgumentsConstant() {
		runMainWithDepentencies_withArguments("index-name", "/path/to/file",
				"too-many-arguments", "too", "many", "arguments");
		verify(runtimeMock).exit(BucketFreezer.EXIT_INCORRECT_ARGUMENTS);
	}

	public void main_fileNotADirectory_exitWithFileNotADirectoryConstant()
			throws IOException {
		File file = createFile();
		assertTrue(!file.isDirectory());
		runMainWithDepentencies_withArguments("index-name", file.getAbsolutePath());
		verify(runtimeMock).exit(BucketFreezer.EXIT_FILE_NOT_A_DIRECTORY);
	}

	public void main_nonExistingFile_exitWithFileNotFoundConstant() {
		File file = TUtilsFile.createFilePath();
		runMainWithDepentencies_withArguments("index-name", file.getAbsolutePath());
		verify(runtimeMock).exit(BucketFreezer.EXIT_FILE_NOT_FOUND);
	}

	private void runMainWithDepentencies_withArguments(String... args) {
		BucketFreezer.runMainWithDependencies(runtimeMock, bucketFreezerProvider,
				mock(RegistersMBeans.class), args);
	}

}
