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
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.FileOverwriteException;
import com.splunk.shuttl.archiver.filesystem.UnsupportedUriException;
import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsFileSystem;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "fast-unit" })
public class HadoopPutTransfererTest {

	private HadoopPutTransferer hadoopPutTransferer;
	private FileSystem fileSystem;
	private URI foo;

	@BeforeMethod
	public void setUp() throws IOException {
		fileSystem = TUtilsFileSystem.getLocalFileSystem();
		hadoopPutTransferer = new HadoopPutTransferer(fileSystem);

		File f = createFileInParent(createDirectory(), "foo");
		f.delete();
		foo = f.toURI();
	}

	public void _givenValidPaths_transferFileToTemp() throws IOException {
		File from = TUtilsFile.createFileInParent(createDirectory(), "source");
		TUtilsFile.populateFileWithRandomContent(from);
		File temp = createFilePath();

		hadoopPutTransferer.transferData(from.toURI(), temp.toURI(), foo);
		TUtilsTestNG.assertFileContentsEqual(from, temp);
	}

	public void _tempExists_deleteRecursivelyAndOverwrite() throws IOException {
		File dirWithOneFile = createDirectory();
		createFileInParent(dirWithOneFile, "x");
		File dirWithTwoFiles = createDirectory();
		createFileInParent(dirWithTwoFiles, "a");
		createFileInParent(dirWithTwoFiles, "b");

		assertTrue(dirWithTwoFiles.exists());
		hadoopPutTransferer.transferData(dirWithOneFile.toURI(),
				dirWithTwoFiles.toURI(), foo);

		Set<String> names = new HashSet<String>();
		for (File f : dirWithTwoFiles.listFiles())
			names.add(f.getName());
		assertTrue(names.contains("x"));
		assertFalse(names.contains("a"));
		assertFalse(names.contains("b"));
	}

	@Test(expectedExceptions = { FileOverwriteException.class })
	public void _givenDestinationAlreadyExists_throws() throws IOException {
		File from = createFile();
		File dstExists = createFile();
		assertTrue(dstExists.exists());
		hadoopPutTransferer.transferData(from.toURI(), foo, dstExists.toURI());
	}

	@Test(expectedExceptions = { FileNotFoundException.class })
	public void _fromDoesNotExist_throws() throws IOException {
		File file = createFile();
		assertTrue(file.delete());
		hadoopPutTransferer.transferData(file.toURI(), foo, foo);
	}

	@Test(expectedExceptions = { UnsupportedUriException.class })
	public void _fromIsNotLocal_throws() throws IOException {
		hadoopPutTransferer.transferData(URI.create("remote://uri"), foo, foo);
	}
}
