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
package com.splunk.shuttl.archiver.filesystem.glacier;

import static java.util.Arrays.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.transaction.file.FileTransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.file.TransfersFiles;

@Test(groups = { "fast-unit" })
public class GlacierArchiveFileSystemHadoopRelayTest {

	private GlacierArchiveFileSystem glacier;
	private HadoopArchiveFileSystem hadoop;
	private String path1;
	private String path2;
	private File file1;
	private File file2;
	private TransfersFiles hadoopTransfersFiles;
	private FileTransactionCleaner hadoopCleansFiles;

	@BeforeMethod
	public void setUp() {
		hadoop = mock(HadoopArchiveFileSystem.class);
		hadoopTransfersFiles = mock(TransfersFiles.class);
		when(hadoop.getFileTransferer()).thenReturn(hadoopTransfersFiles);
		hadoopCleansFiles = mock(FileTransactionCleaner.class);
		when(hadoop.getFileTransactionCleaner()).thenReturn(hadoopCleansFiles);
		glacier = new GlacierArchiveFileSystem(hadoop, null, null, null, null, null);
		path1 = "/path/foo";
		path2 = "/path/bar";
		file1 = mock(File.class);
		file2 = mock(File.class);
	}

	public void rename__relaysToHadoop() throws IOException {
		glacier.rename(path1, path2);
		verify(hadoop).rename(path1, path2);
	}

	public void putFile__relaysToHadoop() throws IOException {
		String local = "local";
		glacier.getFileTransferer().put(local, path1, path2);
		verify(hadoopTransfersFiles).put(local, path1, path2);
	}

	public void getFile__relaysToHadoop() throws IOException {
		glacier.getFileTransferer().get(path1, file1, file2);
		verify(hadoopTransfersFiles).get(path1, file1, file2);
	}

	public void cleanFileTransaction__relaysToHadoop() {
		glacier.getFileTransactionCleaner().cleanTransaction(path1, path2);
		verify(hadoopCleansFiles).cleanTransaction(path1, path2);
	}

	public void mkdirs__relaysToHadoop() throws IOException {
		glacier.mkdirs(path1);
		verify(hadoop).mkdirs(path1);
	}

	public void listPath__relaysToHadoop() throws IOException {
		when(hadoop.listPath(path1)).thenReturn(asList(path2));
		assertEquals(asList(path2), glacier.listPath(path1));
	}

	public void exists__relaysToHadoop() throws IOException {
		glacier.exists(path1);
		verify(hadoop).exists(path1);
	}
}
