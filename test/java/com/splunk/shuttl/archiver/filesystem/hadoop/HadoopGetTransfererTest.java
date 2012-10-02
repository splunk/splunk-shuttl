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

import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.UnsupportedUriException;
import com.splunk.shuttl.testutil.TUtilsFileSystem;

@Test(groups = { "fast-unit" })
public class HadoopGetTransfererTest {

	private HadoopGetTransferer transferer;
	private URI foo;

	@BeforeMethod
	public void setUp() {
		FileSystem localFileSystem = TUtilsFileSystem.getLocalFileSystem();
		transferer = new HadoopGetTransferer(localFileSystem);

		File f = createFileInParent(createDirectory(), "foo");
		f.delete();
		foo = f.toURI();
	}

	public void _givenExistingFileOnHadoopFileSystem_transfersFileToTemp()
			throws IOException {
		File src = createFile();
		File temp = createFileInParent(createDirectory(), "dst");
		assertTrue(temp.delete());

		transferer.transferData(src.toURI(), temp.toURI(), foo);

		assertTrue(src.exists());
		assertTrue(temp.exists());
	}

	@Test(expectedExceptions = { UnsupportedUriException.class })
	public void _tempIsNotLocal_throws() throws IOException {
		transferer.transferData(foo, URI.create("remote://uri"), foo);
	}

	@Test(expectedExceptions = { UnsupportedUriException.class })
	public void _dstIsNotLocal_throws() throws IOException {
		transferer.transferData(foo, foo, URI.create("remote://uri"));
	}

	@Test(expectedExceptions = { FileNotFoundException.class })
	public void _srcDoesNotExist_throws() throws IOException {
		File src = createFilePath();
		assertFalse(src.exists());
		transferer.transferData(src.toURI(), foo, foo);
	}
}
