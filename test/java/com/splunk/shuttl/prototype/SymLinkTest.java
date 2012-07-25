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
package com.splunk.shuttl.prototype;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.testng.annotations.Test;

public class SymLinkTest {

	@Test(groups = { "fast-unit" })
	public void symLink_givenExistingFile_symLinkFileToTheExistingFile()
			throws IOException {
		File existingFile = createFile();
		assertTrue(existingFile.exists());
		File theSymLink = createFilePath();
		assertFalse(theSymLink.exists());
		FileUtil.symLink(existingFile.getAbsolutePath(),
				theSymLink.getAbsolutePath());
		assertTrue(theSymLink.exists());
		assertNotEquals(existingFile.getAbsolutePath(),
				theSymLink.getAbsolutePath());
		assertEquals(existingFile.getCanonicalPath(), theSymLink.getCanonicalPath());
	}
}
