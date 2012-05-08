package com.splunk.shep.archiver.archive;

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

import static com.splunk.shep.archiver.LocalFileSystemConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.recovery.BucketLocker;
import com.splunk.shep.archiver.archive.recovery.BucketMover;
import com.splunk.shep.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shep.testutil.UtilsFile;

/**
 * Fixture: Calling the main method of BucketFreezer. Testing exit codes.
 */
@Test(groups = { "fast-unit" })
public class BucketFreezerSystemExitTest {

    Runtime runtimeMock;
    private BucketFreezer bucketFreezer;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	runtimeMock = mock(Runtime.class);
	bucketFreezer = new BucketFreezer(new BucketMover(getSafeDirectory()),
		new BucketLocker(), mock(ArchiveRestHandler.class),
		mock(FailedBucketsArchiver.class));
    }

    @AfterMethod(groups = { "fast-unit" })
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(getArchiverDirectory());
    }

    @Test(groups = { "fast-unit" })
    public void main_existingDirecotry_returnCode0() throws IOException {
	File directory = UtilsFile.createTempDirectory();
	runMainWithDepentencies_withArguments("index-name",
		directory.getAbsolutePath());
	verify(runtimeMock).exit(0);
    }

    public void main_noArguments_returnCodeMinus1() {
	runMainWithDepentencies_withArguments();
	verify(runtimeMock).exit(-1);
    }

    public void main_oneArgument_returnCodeMinus1() {
	runMainWithDepentencies_withArguments("index-name");
	verify(runtimeMock).exit(-1);
    }

    public void main_threeArguments_returnCodeMinus1() {
	runMainWithDepentencies_withArguments("index-name", "/path/to/file",
		"too-many-arguments");
	verify(runtimeMock).exit(-1);
    }

    public void main_tooManyArguments_returnCodeMinus1() {
	runMainWithDepentencies_withArguments("index-name", "/path/to/file",
		"too-many-arguments", "too", "many", "arguments");
	verify(runtimeMock).exit(-1);
    }

    public void main_fileNotADirectory_returnCodeMinus2() throws IOException {
	File file = File.createTempFile("ArchiveTest", ".tmp");
	file.deleteOnExit();
	assertTrue(!file.isDirectory());
	runMainWithDepentencies_withArguments("index-name",
		file.getAbsolutePath());
	verify(runtimeMock).exit(-2);
    }

    public void main_nonExistingFile_returnMinus3() {
	File file = UtilsFile.createTestFilePath();
	runMainWithDepentencies_withArguments("index-name",
		file.getAbsolutePath());
	verify(runtimeMock).exit(-3);
    }

    private void runMainWithDepentencies_withArguments(String... args) {
	BucketFreezer.runMainWithDependencies(runtimeMock, bucketFreezer, args);
    }

}
