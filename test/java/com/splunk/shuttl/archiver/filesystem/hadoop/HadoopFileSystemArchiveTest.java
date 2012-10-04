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

package com.splunk.shuttl.archiver.filesystem.hadoop;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.FileOverwriteException;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopFileSystemArchive;
import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsFileSystem;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Using the method naming convention:
 * [metodNamn]_[stateUnderTest]_[expectedOutcome]
 */
@Test(groups = { "fast-unit" })
public class HadoopFileSystemArchiveTest {

	private FileSystem fileSystem;
	private HadoopFileSystemArchive hadoopFileSystemArchive;

	@BeforeMethod
	public void beforeMethod() {
		fileSystem = TUtilsFileSystem.getLocalFileSystem();
		hadoopFileSystemArchive = new HadoopFileSystemArchive(fileSystem);
	}

	public void getFile_whenLocalFileAllreadyExist_localFileIsNotOverwritten()
			throws IOException, URISyntaxException {
	}

	public void listPath_listingAPathThatPointsToADirectory_aListThatContainsThePathsInsideSpecifiedDirectory()
			throws URISyntaxException, IOException {
		File dir = createDirectory();
		File file1 = createFileInParent(dir, "file1");
		File file2 = createFileInParent(dir, "file2");

		List<URI> listing = hadoopFileSystemArchive.listPath(dir.toURI());

		assertEquals(2, listing.size());
		assertTrue(listing.contains(file1.toURI()));
		assertTrue(listing.contains(file2.toURI()));
	}

	public void listPath_listingAnEmptyDirectory_emptyList() throws IOException {
		File testDirectory = TUtilsFile.createDirectory();
		List<URI> listing = hadoopFileSystemArchive.listPath(testDirectory.toURI());
		assertEquals(0, listing.size());
	}

	public void listPath_listingAPathThatPointsToAFile_aListOnlyContainingThePathToTheFile()
			throws IOException {
		File file = createFile();
		List<URI> listing = hadoopFileSystemArchive.listPath(file.toURI());
		assertTrue(listing.contains(file.toURI()));
	}

	public void listPath_listingAPathThatDoNotExist_emptyList()
			throws IOException, URISyntaxException {
		File file = createFilePath();
		assertFalse(file.exists());
		List<URI> listing = hadoopFileSystemArchive.listPath(file.toURI());
		assertEquals(0, listing.size());
	}

	public void openFile_existingFileOnHadoop_inputStreamToFile()
			throws FileNotFoundException, IOException {
		File fileWithContent = createFileWithRandomContent();
		List<String> expectedContent = FileUtils.readLines(fileWithContent);

		InputStream openFile = hadoopFileSystemArchive.openFile(fileWithContent
				.toURI());
		assertEquals(expectedContent, IOUtils.readLines(openFile));
	}

	@Test(groups = { "fast-unit" })
	public void mkdirs_givenEmptyDirectory_canMakeDirectoryInTheEmptyOne()
			throws IOException {
		File emptyDir = createDirectory();
		assertTrue(TUtilsFile.isDirectoryEmpty(emptyDir));

		File nextLevelDir = new File(emptyDir, "next-level-dir");
		assertFalse(nextLevelDir.exists());
		hadoopFileSystemArchive.mkdirs(nextLevelDir.toURI());
		assertTrue(nextLevelDir.exists());
	}

	@Test(groups = { "fast-unit" })
	public void mkdirs_givenEmptyDir_canMakeDirsMultipleLevelsDown()
			throws IOException {
		File dir = createDirectory();
		File one = new File(dir, "one");
		File two = new File(one, "two");

		hadoopFileSystemArchive.mkdirs(two.toURI());
		assertTrue(two.exists());
	}

	@Test(groups = { "fast-unit" })
	public void mkdirs_givenExistingDir_doesNothing() throws IOException {
		hadoopFileSystemArchive.mkdirs(createDirectory().toURI());
	}

	@Test(groups = { "fast-unit" })
	public void rename_existingDir_renamesIt() throws IOException {
		File dir = createDirectory();
		File newName = new File(createDirectory(), "foo.bar");
		assertFalse(newName.exists());
		hadoopFileSystemArchive.rename(dir.toURI(), newName.toURI());
		assertTrue(newName.exists());
		assertFalse(dir.exists());
	}

	public void getFile_givenExistingFileOnHadoopFileSystem_transfersFileToTemp()
			throws IOException {
		File src = createFile();
		File temp = createFileInParent(createDirectory(), "temp");
		File dst = createFileInParent(createDirectory(), "dst");
		assertTrue(temp.delete());
		assertTrue(dst.delete());

		hadoopFileSystemArchive.getFile(src.toURI(), temp, dst);

		assertTrue(src.exists());
		assertTrue(temp.exists());
		assertFalse(dst.exists());
	}

	@Test(expectedExceptions = { FileNotFoundException.class })
	public void getFile_srcDoesNotExist_throws() throws IOException {
		File src = createFilePath();
		assertFalse(src.exists());
		hadoopFileSystemArchive.getFile(src.toURI(), createFilePath(),
				createFilePath());
	}

	@Test(expectedExceptions = FileOverwriteException.class)
	public void getFile_whenLocalFileAllreadyExist_fileOverwriteException()
			throws IOException {
		File dst = createFile();
		hadoopFileSystemArchive.getFile(null, null, dst);
	}

	public void putFile_givenValidPaths_transferFileToTemp() throws IOException {
		File from = TUtilsFile.createFileInParent(createDirectory(), "source");
		TUtilsFile.populateFileWithRandomContent(from);
		File temp = createFilePath();
		File dst = createFilePath();

		hadoopFileSystemArchive.putFile(from, temp.toURI(), dst.toURI());
		TUtilsTestNG.assertFileContentsEqual(from, temp);
	}

	public void putFile_tempExists_deleteRecursivelyAndOverwrite()
			throws IOException {
		File dirWithOneFile = createDirectory();
		createFileInParent(dirWithOneFile, "x");
		File dirWithTwoFiles = createDirectory();
		createFileInParent(dirWithTwoFiles, "a");
		createFileInParent(dirWithTwoFiles, "b");

		assertTrue(dirWithTwoFiles.exists());
		hadoopFileSystemArchive.putFile(dirWithOneFile, dirWithTwoFiles.toURI(),
				createFilePath().toURI());

		Set<String> names = new HashSet<String>();
		for (File f : dirWithTwoFiles.listFiles())
			names.add(f.getName());
		assertTrue(names.contains("x"));
		assertFalse(names.contains("a"));
		assertFalse(names.contains("b"));
	}

	@Test(expectedExceptions = { FileOverwriteException.class })
	public void putFile_dstExist_throws() throws IOException {
		File dst = createFile();
		assertTrue(dst.exists());
		hadoopFileSystemArchive.putFile(null, null, dst.toURI());
	}

	@Test(expectedExceptions = FileNotFoundException.class)
	public void putFile_whenLocalFileDoNotExist_fileNotFoundException()
			throws IOException {
		File file = createFilePath();
		assertFalse(file.exists());
		hadoopFileSystemArchive.putFile(file, file.toURI(), file.toURI());
	}

	public void putFile_withDirectoryContainingAnotherDirectory_bothDirectoriesExistsInTheArchive()
			throws IOException {
		File dir1 = createDirectory();
		File dir2 = createDirectoryInParent(dir1, "anotherdir");
		File fileInDir2 = createFileInParent(dir2, "file");
		File tmp = createDirectory();
		hadoopFileSystemArchive
				.putFile(dir1, tmp.toURI(), createFilePath().toURI());
		File transferredDir2 = new File(tmp, dir2.getName());
		assertTrue(transferredDir2.exists());
		assertTrue(new File(transferredDir2, fileInDir2.getName()).exists());
	}
}
