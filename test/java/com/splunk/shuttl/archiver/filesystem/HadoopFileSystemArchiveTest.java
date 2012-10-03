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

package com.splunk.shuttl.archiver.filesystem;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileInputStream;
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
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.HadoopFileSystemPutter;
import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsFileSystem;
import com.splunk.shuttl.testutil.TUtilsPath;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Using the method naming convention:
 * [metodNamn]_[stateUnderTest]_[expectedOutcome]
 */
@Test(groups = { "fast-unit" })
public class HadoopFileSystemArchiveTest {

	private FileSystem fileSystem;
	private HadoopFileSystemArchive hadoopFileSystemArchive;
	private HadoopFileSystemPutter hadoopFileSystemPutter;
	private Path tmpPath;

	@BeforeMethod
	public void beforeMethod() {
		fileSystem = TUtilsFileSystem.getLocalFileSystem();
		tmpPath = new Path("/tmp/" + RandomUtils.nextInt() + "/");
		hadoopFileSystemArchive = new HadoopFileSystemArchive(fileSystem, tmpPath);
		hadoopFileSystemPutter = HadoopFileSystemPutter.create(fileSystem);
	}

	@AfterMethod
	public void afterMethod() throws IOException {
		hadoopFileSystemPutter.deleteMyFiles();
		fileSystem.delete(tmpPath, true);
	}

	@Test(groups = { "fast-unit" })
	public void HadoopFileSystemArchive_notInitialized_aNonNullInstanceIsCreated() {
		// Test done in before
		// Confirm
		assertNotNull(hadoopFileSystemArchive);
	}

	public void getFile_validInput_fileShouldBeRetrived() throws IOException {
		File testFile = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(testFile);
		Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
		URI fileSystemPath = hadoopPath.toUri();
		File retrivedFile = TUtilsFile.createFilePath();

		// Test
		hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);

		// Confirm
		TUtilsTestNG.assertFileContentsEqual(testFile, retrivedFile);
	}

	@Test(expectedExceptions = FileNotFoundException.class)
	public void getFile_whenRemotefileDoNotExist_fileNotFoundException()
			throws IOException, URISyntaxException {
		URI fileSystemPath = new URI("file:///random/path/to/non/existing/file");
		File retrivedFile = TUtilsFile.createFilePath();

		// Test
		hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);
	}

	@Test(expectedExceptions = FileOverwriteException.class)
	public void getFile_whenLocalFileAllreadyExist_fileOverwriteException()
			throws IOException, URISyntaxException {
		File testFile = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(testFile);
		Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
		URI fileSystemPath = hadoopPath.toUri();
		File retrivedFile = TUtilsFile.createFileWithRandomContent();

		// Test
		hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);
	}

	public void getFile_whenLocalFileAllreadyExist_localFileIsNotOverwritten()
			throws IOException, URISyntaxException {
		File testFile = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(testFile);
		Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
		URI fileSystemPath = hadoopPath.toUri();
		File fileThatCouldBeOverwritten = TUtilsFile.createFileWithRandomContent();
		File originalFile = TUtilsFile
				.createFileWithContentsOfFile(fileThatCouldBeOverwritten);

		try {
			// Test
			hadoopFileSystemArchive.getFile(fileThatCouldBeOverwritten,
					fileSystemPath);
		} catch (Exception e) { // Intentionally ignoring.
		}

		// Confirm
		TUtilsTestNG.assertFileContentsEqual(originalFile,
				fileThatCouldBeOverwritten);

	}

	public void putFile_validInput_fileShouldBePutToFilesSystem()
			throws IOException {
		File testFile = TUtilsFile.createFileWithRandomContent();
		Path hadoopPath = TUtilsPath.getSafeDirectory(fileSystem);
		URI fileSystemPath = hadoopPath.toUri();

		// Test
		hadoopFileSystemArchive.putFile(testFile, fileSystemPath);

		// Confirm
		File retrivedFile = TUtilsFileSystem.getFileFromFileSystem(fileSystem,
				hadoopPath);
		TUtilsTestNG.assertFileContentsEqual(testFile, retrivedFile);

	}

	@Test(expectedExceptions = FileNotFoundException.class)
	public void putFile_whenLocalFileDoNotExist_fileNotFoundException()
			throws IOException {
		File testFile = TUtilsFile.createFilePath();
		Path hadoopPath = TUtilsPath.getSafeDirectory(fileSystem);
		URI fileSystemPath = hadoopPath.toUri();

		// Test
		hadoopFileSystemArchive.putFile(testFile, fileSystemPath);
	}

	@Test(expectedExceptions = FileOverwriteException.class)
	public void putFile_whenRemoteFileExists_fileOverwriteException()
			throws IOException {
		File fileThatWouldBeOwerwriten = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
		Path hadoopPath = hadoopFileSystemPutter
				.getPathForFile(fileThatWouldBeOwerwriten);
		URI pathToRemoteFile = hadoopPath.toUri();
		File testFile = TUtilsFile.createFileWithRandomContent();

		// Test
		hadoopFileSystemArchive.putFile(testFile, pathToRemoteFile);
	}

	public void putFile_whenRemoteFileExists_remoteFileShouldNotBeOverwriten()
			throws IOException {
		File fileThatWouldBeOwerwriten = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
		Path hadoopPath = hadoopFileSystemPutter
				.getPathForFile(fileThatWouldBeOwerwriten);
		URI pathToRemoteFile = hadoopPath.toUri();
		File testFile = TUtilsFile.createFileWithRandomContent();

		boolean didGetExeption = false;
		try {
			// Test
			hadoopFileSystemArchive.putFile(testFile, pathToRemoteFile);
		} catch (FileOverwriteException e) {
			didGetExeption = true;
		}

		// Confirm
		assertTrue(didGetExeption);
		File fileAfterPut = TUtilsFile.createFilePath();
		hadoopFileSystemArchive.getFile(fileAfterPut, pathToRemoteFile);
		TUtilsTestNG.assertFileContentsEqual(
				"Put shouln't have overwritten the file.", fileThatWouldBeOwerwriten,
				fileAfterPut);

	}

	public void putFile_withDirectoryContainingAnotherDirectory_bothDirectoriesExistsInTheArchive()
			throws URISyntaxException, FileNotFoundException, FileOverwriteException,
			IOException {
		File parent = TUtilsFile.createDirectory();
		String childFileName = "childDir";
		TUtilsFile.createDirectoryInParent(parent, childFileName);
		Path parentPathOnHadoop = hadoopFileSystemPutter.getPathForFile(parent);
		hadoopFileSystemArchive.putFile(parent, parentPathOnHadoop.toUri());
		assertTrue(fileSystem.exists(parentPathOnHadoop));
		Path childPath = new Path(parentPathOnHadoop, childFileName);
		assertTrue(fileSystem.exists(childPath));
		FileUtils.deleteDirectory(parent);
	}

	public void putFileAtomically_validInput_fileShouldBePutToFilesSystem()
			throws IOException {
		File testFile = TUtilsFile.createFileWithRandomContent();
		Path hadoopPath = TUtilsPath.getSafeDirectory(fileSystem);
		URI fileSystemPath = hadoopPath.toUri();

		// Test
		hadoopFileSystemArchive.putFileAtomically(testFile, fileSystemPath);

		// Confirm
		File retrivedFile = TUtilsFileSystem.getFileFromFileSystem(fileSystem,
				hadoopPath);
		TUtilsTestNG.assertFileContentsEqual(testFile, retrivedFile);

	}

	@Test(expectedExceptions = FileNotFoundException.class)
	public void putFileAtomically_whenLocalFileDoNotExist_fileNotFoundException()
			throws IOException {
		File testFile = TUtilsFile.createFilePath();
		Path hadoopPath = TUtilsPath.getSafeDirectory(fileSystem);
		URI fileSystemPath = hadoopPath.toUri();

		// Test
		hadoopFileSystemArchive.putFileAtomically(testFile, fileSystemPath);
	}

	@Test(expectedExceptions = FileOverwriteException.class)
	public void putFileAtomically_whenRemoteFileExists_fileOverwriteException()
			throws IOException {
		File fileThatWouldBeOwerwriten = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
		Path hadoopPath = hadoopFileSystemPutter
				.getPathForFile(fileThatWouldBeOwerwriten);
		URI pathToRemoteFile = hadoopPath.toUri();
		File testFile = TUtilsFile.createFileWithRandomContent();

		// Test
		hadoopFileSystemArchive.putFileAtomically(testFile, pathToRemoteFile);
	}

	public void putFileAtomically_whenRemoteFileExists_remoteFileShouldNotBeOverwriten()
			throws IOException {
		File fileThatWouldBeOwerwriten = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
		Path hadoopPath = hadoopFileSystemPutter
				.getPathForFile(fileThatWouldBeOwerwriten);
		URI pathToRemoteFile = hadoopPath.toUri();
		File testFile = TUtilsFile.createFileWithRandomContent();

		boolean didGetExeption = false;
		try {
			// Test
			hadoopFileSystemArchive.putFileAtomically(testFile, pathToRemoteFile);
		} catch (FileOverwriteException e) {
			didGetExeption = true;
		}

		// Make sure there was an exception
		assertTrue(didGetExeption);

		// Confirm
		File fileAfterPut = TUtilsFile.createFilePath();
		hadoopFileSystemArchive.getFile(fileAfterPut, pathToRemoteFile);
		TUtilsTestNG.assertFileContentsEqual(
				"Put shouln't have overwritten the file.", fileThatWouldBeOwerwriten,
				fileAfterPut);

	}

	public void putFileAtomically_withDirectoryContainingAnotherDirectory_bothDirectoriesExistsInTheArchive()
			throws URISyntaxException, FileNotFoundException, FileOverwriteException,
			IOException {
		File parent = TUtilsFile.createDirectory();
		String childFileName = "childDir";
		TUtilsFile.createDirectoryInParent(parent, childFileName);
		Path parentPathOnHadoop = hadoopFileSystemPutter.getPathForFile(parent);
		hadoopFileSystemArchive.putFileAtomically(parent,
				parentPathOnHadoop.toUri());
		assertTrue(fileSystem.exists(parentPathOnHadoop));
		Path childPath = new Path(parentPathOnHadoop, childFileName);
		assertTrue(fileSystem.exists(childPath));
		FileUtils.deleteDirectory(parent);
	}

	public void putFileAtomically_withFileAllreadyInTmpFolder_theFilesinTmpFolderDoesNotAffectTheTrasfer()
			throws FileNotFoundException, FileOverwriteException, IOException {
		File fileToTransfer = TUtilsFile.createFileWithRandomContent();
		File fileToPutOnTempThatShouldNotAffectTheTransfer = TUtilsFile
				.createFileWithRandomContent();
		hadoopFileSystemPutter
				.putFile(fileToPutOnTempThatShouldNotAffectTheTransfer);
		Path hadoopPath = TUtilsPath.getSafeDirectory(fileSystem);
		hadoopPath = new Path(hadoopPath, "fileName");

		URI fileSystemPath = hadoopPath.toUri();

		// Test
		hadoopFileSystemArchive.putFileAtomically(fileToTransfer, fileSystemPath);

		// Confirm
		File retrivedFile = TUtilsFileSystem.getFileFromFileSystem(fileSystem,
				hadoopPath);
		TUtilsTestNG.assertFileContentsEqual(fileToTransfer, retrivedFile);

	}

	public void listPath_listingAPathThatPointsToADirectory_aListThatContainsThePathsInsideSpecifiedDirectory()
			throws URISyntaxException, IOException {
		File file1 = TUtilsFile.createFileWithRandomContent();
		File file2 = TUtilsFile.createFileWithRandomContent();
		File file3 = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(file1);
		hadoopFileSystemPutter.putFile(file2);
		hadoopFileSystemPutter.putFile(file3);
		URI baseURI = hadoopFileSystemPutter.getPathOfMyFiles().toUri();
		URI uri1 = new URI(baseURI + "/" + file1.getName());
		URI uri2 = new URI(baseURI + "/" + file2.getName());
		URI uri3 = new URI(baseURI + "/" + file3.getName());

		// Test
		List<URI> contents = hadoopFileSystemArchive.listPath(baseURI);

		// Confirm
		assertTrue(contents.contains(uri1));
		assertTrue(contents.contains(uri2));
		assertTrue(contents.contains(uri3));
	}

	public void listPath_listingAnEmptyDirectory_emptyList() throws IOException {
		File testDirectory = TUtilsFile.createDirectory();
		hadoopFileSystemPutter.putFile(testDirectory);
		URI hadoopPathToTheDirectory = hadoopFileSystemPutter.getPathForFile(
				testDirectory).toUri();

		// Test
		List<URI> contents = hadoopFileSystemArchive
				.listPath(hadoopPathToTheDirectory);

		// Confirm
		assertEquals(0, contents.size());
	}

	public void listPath_listingAPathThatPointsToAFile_aListOnlyContainingThePathToTheFile()
			throws IOException {
		File file = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(file);
		URI uri = hadoopFileSystemPutter.getPathForFile(file).toUri();

		// Test
		List<URI> contents = hadoopFileSystemArchive.listPath(uri);

		// Confirm
		assertTrue(contents.contains(uri));
	}

	public void listPath_listingAPathThatDoNotExist_emptyList()
			throws IOException, URISyntaxException {
		URI hadoopPathToTheDirectory = new URI("file:///This/path/should/not/exist");

		// Test
		List<URI> contents = hadoopFileSystemArchive
				.listPath(hadoopPathToTheDirectory);

		// Confirm
		assertEquals(0, contents.size());
	}

	public void deletePathRecursivly_givenADirectory_thePathShouldBeDeleted()
			throws IOException {
		File testDirectory = TUtilsFile.createDirectory();
		hadoopFileSystemPutter.putFile(testDirectory);
		Path testFilePath = hadoopFileSystemPutter.getPathForFile(testDirectory);

		// Make sure setup was correct
		assertTrue(fileSystem.exists(testFilePath));

		// Test
		hadoopFileSystemArchive.deletePathRecursivly(testFilePath);

		// Verify
		assertFalse(fileSystem.exists(testFilePath));
	}

	public void deletePathRecursivly_givenADirectoryWithFilesInIt_thePathShouldBeDeleted()
			throws IOException {
		File testDirectory = TUtilsFile.createDirectory();
		File testFile = TUtilsFile.createFileInParent(testDirectory, "STUFF");
		TUtilsFile.populateFileWithRandomContent(testFile);
		hadoopFileSystemPutter.putFile(testDirectory);
		Path testFilePath = hadoopFileSystemPutter.getPathForFile(testDirectory);

		// Make sure setup was correct
		assertTrue(fileSystem.exists(testFilePath));
		assertTrue(fileSystem.exists(testFilePath.suffix("/STUFF")));

		// Test
		hadoopFileSystemArchive.deletePathRecursivly(testFilePath);

		// Verify
		assertFalse(fileSystem.exists(testFilePath));
		assertFalse(fileSystem.exists(testFilePath.suffix("STUFF")));

	}

	public void putFileToTmpDirectoryAppendingPath_existingFile_fileIsCopiedToTheTmpDirectory()
			throws IOException {
		File testFile = TUtilsFile.createFileWithRandomContent();
		Path testFilePath = new Path("/just/a/random/path");
		Path whereTestFileShouldGo = new Path(tmpPath.toUri().getPath()
				+ testFilePath.toUri().getPath());

		// Make sure setup was correct
		assertFalse(fileSystem.exists(whereTestFileShouldGo));
		assertFalse(fileSystem.exists(testFilePath));

		// Test
		Path pathWhereTestFilePut = hadoopFileSystemArchive
				.putFileToTmpOverwritingOldFiles(testFile, testFilePath.toUri());

		// Verify
		assertEquals(whereTestFileShouldGo, pathWhereTestFilePut);
		assertTrue(fileSystem.exists(whereTestFileShouldGo));
		assertFalse(fileSystem.exists(testFilePath));
	}

	public void move_existingFileOnHadoop_fileIsMoved() throws IOException {
		File testFile = TUtilsFile.createFileWithRandomContent();
		hadoopFileSystemPutter.putFile(testFile);
		Path testFilePath = hadoopFileSystemPutter.getPathForFile(testFile);
		Path testFilePathAfterMoving = new Path(tmpPath.toUri().getPath()
				+ testFilePath.toUri().getPath());

		// Make sure setup was correct
		assertTrue(fileSystem.exists(testFilePath));
		assertFalse(fileSystem.exists(testFilePathAfterMoving));

		// Test
		hadoopFileSystemArchive.move(testFilePath, testFilePathAfterMoving);

		// Verify
		assertFalse(fileSystem.exists(testFilePath));
		assertTrue(fileSystem.exists(testFilePathAfterMoving));

	}

	public void openFile_existingFileOnHadoop_inputStreamToFile()
			throws FileNotFoundException, IOException {
		File fileWithRandomContent = createFileWithRandomContent();
		List<String> expectedContent = IOUtils.readLines(new FileInputStream(
				fileWithRandomContent));

		hadoopFileSystemPutter.putFile(fileWithRandomContent);
		Path pathToFile = hadoopFileSystemPutter
				.getPathForFile(fileWithRandomContent);

		InputStream openFile = hadoopFileSystemArchive.openFile(pathToFile.toUri());
		List<String> actualContent = IOUtils.readLines(openFile);

		assertEquals(expectedContent, actualContent);
	}

	/** ------- New transaction file system ------- **/

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

}
