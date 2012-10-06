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

import java.io.File;
import java.net.URI;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Factory class for creating Transactions.
 */
public class TransactionProvider {

	private TransactionalFileSystem fs;

	public TransactionProvider(TransactionalFileSystem fs) {
		this.fs = fs;
	}

	public Transaction bucketPut(Bucket src, URI temp, URI dst) {
		return createPut(fs, src, temp, dst);
	}

	public Transaction bucketGet(Bucket src, URI temp, URI dst) {
		return createGet(fs, src, temp, dst);
	}

	public Transaction filePut(File src, URI temp, URI dst) {
		return createPut(fs, src.toURI(), temp, dst);
	}

	public Transaction fileGet(File src, URI temp, URI dst) {
		return createGet(fs, src.toURI(), temp, dst);
	}

	private static TransactionalFileSystem getLocalTransactionalFileSystem() {
		return ArchiveFileSystemFactory.getWithUriAndLocalFileSystemPaths(
				URI.create("file:/"), null);
	}

	public static Transaction createPut(TransactionalFileSystem tfs,
			Bucket bucket, URI temp, URI dst) {
		return create(tfs, tfs, bucket, temp, dst);
	}

	public static Transaction createGet(TransactionalFileSystem tfs,
			Bucket bucket, URI temp, URI dst) {
		return create(tfs, getLocalTransactionalFileSystem(), bucket, temp, dst);
	}

	private static Transaction create(TransactionalFileSystem sender,
			TransactionalFileSystem reciever, Bucket bucket, URI temp, URI dst) {
		return new Transaction(reciever, new DataTransferer(sender, sender),
				bucket, temp, dst);
	}

	public static Transaction createPut(TransactionalFileSystem tfs, URI src,
			URI temp, URI dst) {
		return create(tfs, tfs, src, temp, dst);
	}

	public static Transaction createGet(TransactionalFileSystem tfs, URI src,
			URI temp, URI dst) {
		return create(tfs, getLocalTransactionalFileSystem(), src, temp, dst);
	}

	private static Transaction create(TransactionalFileSystem sender,
			TransactionalFileSystem reciever, URI src, URI temp, URI dst) {
		return new Transaction(reciever, new DataTransferer(sender, sender), src,
				temp, dst);
	}
}
