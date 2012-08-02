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
package com.splunk.shuttl.testutil;

import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class TUtilsConfTest {

	private Set<String> confFileNames;

	@BeforeMethod
	public void setUp() {
		confFileNames = new HashSet<String>();
		confFileNames.addAll(asList("archiver.xml", "server.xml", "splunk.xml"));
	}

	public void getNullsConfsDir_givenConfsAsTestResource_getsDirToConfigurationFiles() {
		File nullConfsDir = TUtilsConf.getNullConfsDir();

		// Assert
		File[] listFiles = nullConfsDir.listFiles();
		assertEquals(3, listFiles.length);
		assertFilesAreExpectedConFiles(listFiles);
	}

	/**
	 * assert knows a lot about the configuration files. Better way to assert?
	 */
	private void assertFilesAreExpectedConFiles(File[] listFiles) {
		for (File f : listFiles)
			if (!confFileNames.contains(f.getName()))
				fail("file is something else than expected conf file. File: " + f);
	}
}
