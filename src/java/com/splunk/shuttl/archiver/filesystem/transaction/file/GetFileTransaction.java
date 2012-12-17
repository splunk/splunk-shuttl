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
package com.splunk.shuttl.archiver.filesystem.transaction.file;

import java.io.File;
import java.io.IOException;

import com.splunk.shuttl.archiver.filesystem.transaction.AbstractTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.HasFileStructure;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.LocalTransactionalFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionalFileSystem;

/**
 * 
 */
public class GetFileTransaction extends AbstractTransaction<String> {

	private TransfersFiles transfersFiles;

	protected GetFileTransaction(TransfersFiles transfersFiles,
			HasFileStructure hasFileStructure,
			TransactionCleaner<String> transactionCleaner, String data, String temp,
			String dst) {
		super(hasFileStructure, transactionCleaner, data, temp, dst);
		this.transfersFiles = transfersFiles;
	}

	@Override
	protected void doTransferData(String data, String temp, String dst)
			throws IOException {
		transfersFiles.get(data, new File(temp), new File(dst));
	}

	public static GetFileTransaction create(TransactionalFileSystem fs,
			String src, String temp, String dst) {
		return new GetFileTransaction(fs.getFileTransferer(),
				LocalTransactionalFileSystemFactory.create(),
				fs.getFileTransactionCleaner(), src, temp, dst);
	}
}
