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
package com.splunk.shuttl.archiver.filesystem;

import static org.testng.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsFunctional;

public class HadoopFileSystemArchiveSlowTest {

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "hadoop.host", "hadoop.port" })
	public void rename_dirWithMultipleLevelsOfNonExistingFiles_renamesDirectory(
			String hadoopHost, String hadoopPort) throws IOException {
		FileSystem hadoopFileSystem = TUtilsFunctional.getHadoopFileSystem(
				hadoopHost, hadoopPort);
		String simpleClassName = getClass().getSimpleName();
		Path path = new Path(simpleClassName + "/1/foo/dir/")
				.makeQualified(hadoopFileSystem);
		Path otherRoot = new Path(simpleClassName + "/2/foo/dir")
				.makeQualified(hadoopFileSystem);

		HadoopFileSystemArchive realFileStructure = new HadoopFileSystemArchive(
				hadoopFileSystem);
		try {
			hadoopFileSystem.mkdirs(path);
			assertTrue(hadoopFileSystem.exists(path));
			hadoopFileSystem.delete(otherRoot, true);
			assertFalse(hadoopFileSystem.exists(otherRoot));

			// Test
			realFileStructure.rename(path.toUri(), otherRoot.toUri());

			assertTrue(hadoopFileSystem.exists(otherRoot));
			assertFalse(hadoopFileSystem.exists(path));
		} finally {
			hadoopFileSystem.delete(new Path("/1"), true);
			hadoopFileSystem.delete(new Path("/2"), true);
		}
	}
}
