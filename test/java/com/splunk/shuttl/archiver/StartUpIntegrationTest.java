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
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class StartUpIntegrationTest {

	public void _givenFilesInThawLocksLocation_cleanThem() {
		testIntegrationWithDirectory(new LocalFileSystemConstants()
				.getThawLocksDirectory());
	}

	private void testIntegrationWithDirectory(File directoryToIntegrate) {
		createDirectoryInParent(directoryToIntegrate, "somefile");
		StartUpCleaner.create().clean();
		assertTrue(isDirectoryEmpty(directoryToIntegrate));
	}

	public void _givenFilesInThawTransfersLocation_cleansThem() {
		testIntegrationWithDirectory(new LocalFileSystemConstants()
				.getThawTransfersDirectory());
	}
}
