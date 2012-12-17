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
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "end-to-end" })
public class HadoopArchiveFileSystemFactoryTest {

	@Parameters(value = { "hadoop.host", "hadoop.port" })
	public void create_givenPropertyFile_createsInstance(String hadoopHost,
			String hadoopPort) throws IOException {

		File hdfsProperties = createFile();
		TUtilsFile.writeKeyValueProperties(hdfsProperties, "hadoop.host = "
				+ hadoopHost, "hadoop.port = " + hadoopPort);

		HadoopArchiveFileSystem hdfs = HadoopArchiveFileSystemFactory
				.createWithPropertyFile(hdfsProperties);
		FileSystem fs = hdfs.getFileSystem();
		assertEquals(hadoopHost, fs.getUri().getHost());
		assertEquals(hadoopPort, "" + fs.getUri().getPort());
	}

}
