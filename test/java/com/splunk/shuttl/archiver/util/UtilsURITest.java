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
package com.splunk.shuttl.archiver.util;

import static org.testng.AssertJUnit.*;

import java.net.URI;

import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.util.UtilsURI;

@Test(groups = { "fast-unit" })
public class UtilsURITest {

    /**
     * <pre>
     * "file:/a/b/c" -> "c"
     * </pre>
     */
    @Test(groups = { "fast-unit" })
    public void getFileNameWithTrimmedEndingFileSeparator_uriToDirWithoutEndingFileSeparator_dirNameOnly() {
	URI uriToTest = URI.create("file:/a/b/c");
	String baseName = UtilsURI
		.getFileNameWithTrimmedEndingFileSeparator(uriToTest);
	assertEquals("c", baseName);
    }

    /**
     * <pre>
     * "file:/a/b/c/" -> "c"
     * </pre>
     */
    public void getFileNameWithTrimmedEndingFileSeparator_uriToDirWithEndingFileSeparator_dirNameOnly() {
	URI uriToTest = URI.create("file:/a/b/c/");
	String baseName = UtilsURI
		.getFileNameWithTrimmedEndingFileSeparator(uriToTest);
	assertEquals("c", baseName);
    }

    /**
     * <pre>
     * "file:/a/b/c.txt" -> "c.txt"
     * </pre>
     */
    public void getFileNameWithTrimmedEndingFileSeparator_uriToTxtFile_txtFileOnly() {
	URI uriToTest = URI.create("file:/a/b/c.txt");
	String baseName = UtilsURI
		.getFileNameWithTrimmedEndingFileSeparator(uriToTest);
	assertEquals("c.txt", baseName);
    }

}
