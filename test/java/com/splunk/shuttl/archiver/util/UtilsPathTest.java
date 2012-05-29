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

package com.splunk.shuttl.archiver.util;

import static org.testng.AssertJUnit.*;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class UtilsPathTest {
	public void createPathByAppending_pathsWithSchemes_theNewPathShouldProperlyBeCreated() {
		Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp");
		Path pathToAppend = new Path("hdfs://localhost:9000/an/other/path");

		// Test
		Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended,
				pathToAppend);

		// Verify
		assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path"), newPath);
	}

	@Test
	public void createPathByAppending_pathsWithSchemesAndEndingSlahs_theNewPathShouldProperlyBeCreated() {
		Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp/");
		Path pathToAppend = new Path("hdfs://localhost:9000/an/other/path/");

		// Test
		Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended,
				pathToAppend);

		// Verify
		assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path/"), newPath);
	}

	@Test
	public void createPathByAppending_appendPathHavingDiffrentScheme_schemeOfTheToBeAppededPathShouldBeUsed() {
		Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp/");
		Path pathToAppend = new Path("file://localhost:9000/an/other/path/");

		// Test
		Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended,
				pathToAppend);

		// Verify
		assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path/"), newPath);
	}

	@Test
	public void createPathByAppending_appendPathWithOutScheme_schemeOfTheToBeAppededPathShouldBeUsed() {
		Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp/");
		Path pathToAppend = new Path("/an/other/path/");

		// Test
		Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended,
				pathToAppend);

		// Verify
		assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path/"), newPath);
	}
}
