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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.importexport.tgz.TgzImporter.TgzImportFailedException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "slow-unit" })
public class TgzImporterTest {

	private TgzImporter tgzImporter;
	private LocalBucket tgzBucket;

	@BeforeMethod
	public void setUp() {
		tgzBucket = TUtilsBucket.createRealSplunkBucketTgz();
		tgzImporter = new TgzImporter(ShellExecutor.getInstance());
	}

	public void _givenTgzAndOriginalBucket_importsTgzBucketToBeEqualToOriginal() {
		LocalBucket original = TUtilsBucket.createRealBucket();

		LocalBucket imported = tgzImporter.importBucket(tgzBucket);
		assertEquals(original.getFormat(), imported.getFormat());
		assertEquals(numberOfFiles(original), numberOfFiles(imported));
	}

	private int numberOfFiles(LocalBucket original) {
		return original.getDirectory().listFiles().length;
	}

	@SuppressWarnings("unchecked")
	@Test(expectedExceptions = { TgzImportFailedException.class })
	public void _unsuccessfulImport_throws() {
		ShellExecutor shellExecutor = mock(ShellExecutor.class);
		when(shellExecutor.executeCommand(anyMap(), anyList())).thenReturn(3);
		new TgzImporter(shellExecutor).importBucket(tgzBucket);
	}
}
