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
package com.splunk.shuttl.archiver.importexport.tgz;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.HashMap;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class CreatesBucketTgzTest {

	private CreatesBucketTgz createsBucketTgz;
	private ShellExecutor shellExecutor;
	private GetsBucketsExportFile getsBucketsExportFile;

	private HashMap<String, String> emptyMap;
	private Bucket bucket;
	private File tar;
	private File tgz;

	@BeforeMethod
	public void setUp() {
		shellExecutor = mock(ShellExecutor.class);
		getsBucketsExportFile = mock(GetsBucketsExportFile.class);
		createsBucketTgz = new CreatesBucketTgz(shellExecutor,
				getsBucketsExportFile);

		emptyMap = new HashMap<String, String>();
		bucket = TUtilsBucket.createBucket();
		tar = createFile();
		tgz = createFile();
		when(getsBucketsExportFile.getExportFile(bucket, "tar")).thenReturn(tar);
		when(getsBucketsExportFile.getExportFile(bucket, "tgz")).thenReturn(tgz);
	}

	public void _givenBucket_createsTarWithBucketNameAndTarExtension() {
		String tarBucketCmd = "tar -c " + bucket.getDirectory().getAbsolutePath()
				+ " > " + tar.getAbsolutePath();
		String gzipCmd = "gzip -c " + tar.getAbsolutePath() + " > "
				+ tgz.getAbsolutePath();
		String[] cmd = { "/bin/sh", "-c", tarBucketCmd + " && " + gzipCmd };

		when(shellExecutor.executeCommand(emptyMap, asList(cmd))).thenReturn(0);

		assertNotNull(createsBucketTgz.createTgz(bucket));
		assertFalse(tar.exists());
	}

	@SuppressWarnings("unchecked")
	public void _tarFails_throwsAndNoTrashFilesExist() {
		when(shellExecutor.executeCommand(anyMap(), anyList())).thenReturn(1);

		try {
			createsBucketTgz.createTgz(bucket);
		} catch (RuntimeException e) {
			assertFalse(tar.exists());
			assertFalse(tgz.exists());
		}
	}
}
