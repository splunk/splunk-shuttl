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
package com.splunk.shuttl.archiver;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "fast-unit" })
public class StartUpCleanerTest {

	@Test(groups = { "fast-unit" })
	public void _givenDirectories_deletesAllSubDirectoriesOfThem() {
		File dir1 = createDirectory();
		createDirectoryInParent(dir1, "somedir");
		File dir2 = createDirectory();
		createDirectoryInParent(dir2, "someDir2");
		StartUpCleaner startUpCleaner = new StartUpCleaner(asList(dir1, dir2));
		startUpCleaner.clean();
		assertTrue(TUtilsFile.isDirectoryEmpty(dir1));
		assertTrue(TUtilsFile.isDirectoryEmpty(dir1));
	}

	public void _givenEmptyDirectory_doesNotCrash() {
		StartUpCleaner startUpCleaner = new StartUpCleaner(
				asList(createDirectory()));
		startUpCleaner.clean();
	}

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void _givenFile_throwsIllegalArugment() {
		new StartUpCleaner(asList(createFile()));
	}

}
