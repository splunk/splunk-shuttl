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
package com.splunk.shuttl.archiver.filesystem.transaction;

import java.net.URI;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.GetBucketTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.PutBucketTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.file.GetFileTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.file.PutFileTransaction;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Factory class for creating Transactions.
 */
public class TransactionProvider {

	public static TransactionalFileSystem getLocalTransactionalFileSystem() {
		return ArchiveFileSystemFactory.getWithUriAndLocalFileSystemPaths(
				URI.create("file:/"), null);
	}

	public static Transaction createPut(TransactionalFileSystem tfs, Bucket src,
			String temp, String dst) {
		return PutBucketTransaction.create(tfs, src, temp, dst);
	}

	public static Transaction createGet(TransactionalFileSystem tfs,
			Bucket bucket, String temp, String dst) {
		return GetBucketTransaction.create(tfs, bucket, temp, dst);
	}

	public static Transaction createPut(TransactionalFileSystem tfs, String src,
			String temp, String dst) {
		return PutFileTransaction.create(tfs, src, temp, dst);
	}

	public static Transaction createGet(TransactionalFileSystem tfs, String src,
			String temp, String dst) {
		return GetFileTransaction.create(tfs, src, temp, dst);
	}
}
