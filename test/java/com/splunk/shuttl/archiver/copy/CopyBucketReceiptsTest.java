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
package com.splunk.shuttl.archiver.copy;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class CopyBucketReceiptsTest {

	private CopyBucketReceipts bucketReceipts;
	private LocalFileSystemPaths fileSystemPaths;
	private LocalBucket bucket;

	@BeforeMethod
	public void setUp() {
		fileSystemPaths = new LocalFileSystemPaths(createDirectory());
		bucketReceipts = new CopyBucketReceipts(fileSystemPaths);
		bucket = TUtilsBucket.createBucket();
	}

	public void createReceipt_bucket_receiptExists() {
		File receipt = bucketReceipts.createReceipt(bucket);
		assertTrue(receipt.exists());
	}

	public void createReceipt_givenBucket_getsReceiptInReceiptDirectory() {
		File receipt = bucketReceipts.createReceipt(bucket);
		File expectedReceiptParent = fileSystemPaths
				.getCopyBucketReceiptsDirectory(bucket);
		assertEquals(receipt.getParentFile().getAbsolutePath(),
				expectedReceiptParent.getAbsolutePath());
	}

	public void hasReceipt_notCreatedReceipt_false() {
		assertFalse(bucketReceipts.hasReceipt(bucket));
	}

	public void hasReceipt_createdReceipt_true() {
		bucketReceipts.createReceipt(bucket);
		assertTrue(bucketReceipts.hasReceipt(bucket));
	}
}
