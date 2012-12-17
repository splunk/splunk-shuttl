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

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.BucketTransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.TransfersBuckets;
import com.splunk.shuttl.archiver.filesystem.transaction.file.FileTransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.file.TransfersFiles;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Transactions for putting and getting {@link Bucket}s and their metadata. <br/>
 * TODO: Merge with {@link ArchiveFileSystem}.
 */
public interface TransactionalFileSystem extends HasFileStructure {

	TransfersBuckets getBucketTransferer();

	TransfersFiles getFileTransferer();

	BucketTransactionCleaner getBucketTransactionCleaner();

	FileTransactionCleaner getFileTransactionCleaner();

}
