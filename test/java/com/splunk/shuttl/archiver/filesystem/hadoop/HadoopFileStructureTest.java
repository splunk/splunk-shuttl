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
package com.splunk.shuttl.archiver.filesystem.hadoop;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsFileSystem;
import com.splunk.shuttl.testutil.TUtilsFunctional;

public class HadoopFileStructureTest {

	private HadoopFileStructure hadoopFileStructure;
	private FileSystem fileSystem;

	@BeforeMethod(alwaysRun = true)
	public void setUp() {
		fileSystem = TUtilsFileSystem.getLocalFileSystem();
		hadoopFileStructure = new HadoopFileStructure(fileSystem);
	}

	@Test(groups = { "fast-unit" })
	public void mkdirs_givenEmptyDirectory_canMakeDirectoryInTheEmptyOne()
			throws IOException {
		File emptyDir = createDirectory();
		assertTrue(TUtilsFile.isDirectoryEmpty(emptyDir));

		File nextLevelDir = new File(emptyDir, "next-level-dir");
		assertFalse(nextLevelDir.exists());
		hadoopFileStructure.mkdirs(nextLevelDir.toURI());
		assertTrue(nextLevelDir.exists());
	}

	@Test(groups = { "fast-unit" })
	public void mkdirs_givenEmptyDir_canMakeDirsMultipleLevelsDown()
			throws IOException {
		File dir = createDirectory();
		File one = new File(dir, "one");
		File two = new File(one, "two");

		hadoopFileStructure.mkdirs(two.toURI());
		assertTrue(two.exists());
	}

	@Test(groups = { "fast-unit" })
	public void mkdirs_givenExistingDir_doesNothing() throws IOException {
		hadoopFileStructure.mkdirs(createDirectory().toURI());
	}

	@Test(groups = { "fast-unit" })
	public void rename_existingDir_renamesIt() throws IOException {
		File dir = createDirectory();
		File newName = new File(createDirectory(), "foo.bar");
		assertFalse(newName.exists());
		hadoopFileStructure.rename(dir.toURI(), newName.toURI());
		assertTrue(newName.exists());
		assertFalse(dir.exists());
	}

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

		HadoopFileStructure realFileStructure = new HadoopFileStructure(
				hadoopFileSystem);
		try {
			hadoopFileSystem.mkdirs(path);
			assertTrue(hadoopFileSystem.exists(path));
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
