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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.FileOverwriteException;
import com.splunk.shuttl.archiver.filesystem.transaction.file.TransfersFiles;
import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsFileSystem;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "fast-unit" })
public class HadoopArchiveFileSystemFileTransferTest {

	private FileSystem fileSystem;
	private TransfersFiles hadoopTransfersFiles;

	@BeforeMethod
	public void setup() {
		fileSystem = TUtilsFileSystem.getLocalFileSystem();
		hadoopTransfersFiles = new HadoopArchiveFileSystem(fileSystem)
				.getFileTransferer();
	}

	public void getFile_givenExistingFileOnHadoopFileSystem_transfersFileToTemp()
			throws IOException {
		File src = createFile();
		File temp = createFileInParent(createDirectory(), "temp");
		File dst = createFileInParent(createDirectory(), "dst");
		assertTrue(temp.delete());
		assertTrue(dst.delete());

		hadoopTransfersFiles.get(src.getAbsolutePath(), temp, dst);

		assertTrue(src.exists());
		assertTrue(temp.exists());
		assertFalse(dst.exists());
	}

	@Test(expectedExceptions = { FileNotFoundException.class })
	public void getFile_srcDoesNotExist_throws() throws IOException {
		File src = createFilePath();
		assertFalse(src.exists());
		hadoopTransfersFiles.get(src.getAbsolutePath(), createFilePath(),
				createFilePath());
	}

	@Test(expectedExceptions = FileOverwriteException.class)
	public void getFile_whenLocalFileAllreadyExist_fileOverwriteException()
			throws IOException {
		File dst = createFile();
		hadoopTransfersFiles.get("foo", createFile(), dst);
	}

	public void putFile_givenValidPaths_transferFileToTemp() throws IOException {
		File from = TUtilsFile.createFileInParent(createDirectory(), "source");
		TUtilsFile.populateFileWithRandomContent(from);
		File temp = createFilePath();
		File dst = createFilePath();

		hadoopTransfersFiles.put(from.getAbsolutePath(), temp.getAbsolutePath(),
				dst.getAbsolutePath());
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
		hadoopTransfersFiles.put(dirWithOneFile.getAbsolutePath(),
				dirWithTwoFiles.getAbsolutePath(), createFilePath().getAbsolutePath());

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
		hadoopTransfersFiles.put("foo", "foo", dst.getAbsolutePath());
	}

	@Test(expectedExceptions = FileNotFoundException.class)
	public void putFile_whenLocalFileDoNotExist_fileNotFoundException()
			throws IOException {
		File file = createFilePath();
		assertFalse(file.exists());
		hadoopTransfersFiles.put(file.getAbsolutePath(), file.getAbsolutePath(),
				file.getAbsolutePath());
	}

	public void putFile_withDirectoryContainingAnotherDirectory_bothDirectoriesExistsInTheArchive()
			throws IOException {
		File dir1 = createDirectory();
		File dir2 = createDirectoryInParent(dir1, "anotherdir");
		File fileInDir2 = createFileInParent(dir2, "file");
		File tmp = createDirectory();
		hadoopTransfersFiles.put(dir1.getAbsolutePath(), tmp.getAbsolutePath(),
				createFilePath().getAbsolutePath());
		File transferredDir2 = new File(tmp, dir2.getName());
		assertTrue(transferredDir2.exists());
		assertTrue(new File(transferredDir2, fileInDir2.getName()).exists());
	}

	public void putFile_givenDirectoryAsTemp_putsFileAsThatDirectory()
			throws IOException {
		File dir = createDirectory();
		File temp = new File(dir, "foo/baz/bar");
		assertTrue(temp.mkdirs());
		File source = createDirectory();
		File fileInSource = createFileInParent(source, "fisk");

		hadoopTransfersFiles.put(source.getAbsolutePath(), temp.getAbsolutePath(),
				createFilePath().getAbsolutePath());

		assertDirContainsFile(temp, fileInSource);
	}

	public void getFile_givenExistingDirAsTemp_getsFileAsThatDirectory()
			throws IOException {
		File dir = createDirectory();
		File temp = new File(dir, "foo/baz/bar");
		assertTrue(temp.mkdirs());
		File source = createDirectory();
		File fileInSource = createFileInParent(source, "fisk");

		hadoopTransfersFiles.get(source.getAbsolutePath(), temp, createFilePath());

		assertDirContainsFile(temp, fileInSource);

	}

	private void assertDirContainsFile(File temp, File fileInSource) {
		HashSet<String> files = new HashSet<String>();
		for (File f : temp.listFiles())
			files.add(f.getName());
		assertTrue(files.contains(fileInSource.getName()));
	}
}
