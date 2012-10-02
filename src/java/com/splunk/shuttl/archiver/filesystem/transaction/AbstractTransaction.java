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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.net.URI;

import org.apache.log4j.Logger;

/**
 * Provides the generic method calls for doing a TransactionalTransfer.
 */
public abstract class AbstractTransaction implements Transaction {

	private static final Logger logger = Logger
			.getLogger(AbstractTransaction.class);

	protected final HasFileStructure hasFileStructure;
	protected final DataTransfer dataTransfer;
	protected final URI from;
	protected final URI remoteTemp;
	protected final URI to;

	public AbstractTransaction(HasFileStructure hasFileStructure,
			DataTransfer dataTransfer, URI from, URI remoteTemp, URI to) {
		this.hasFileStructure = hasFileStructure;
		this.dataTransfer = dataTransfer;
		this.from = from;
		this.remoteTemp = remoteTemp;
		this.to = to;
	}

	@Override
	public void prepare() {
		makeDirectories();
		transferData();
	}

	private void makeDirectories() {
		try {
			hasFileStructure.mkdirs(remoteTemp);
		} catch (IOException e) {
			logger.error(did("Tried making directories up to: " + remoteTemp, e,
					"To make directories.", "uri", remoteTemp));
			throw new TransactionException(e);
		}
	}

	protected void transferData() {
		try {
			dataTransfer.transferData(from, remoteTemp, to);
		} catch (IOException e) {
			logger.error(did("Transferred data to DataTransferer: " + dataTransfer,
					e, "To transfer file to remote file system via temp.", "from", from,
					"to", to, "temp", remoteTemp));
			throw new TransactionException(e);
		}
	}

	@Override
	public void commit() {
		try {
			hasFileStructure.rename(remoteTemp, to);
		} catch (IOException e) {
			logger
					.error(did("Tried commiting transaction", e,
							"To complete transaction", "from", from, "to", to, "temp",
							remoteTemp));
			throw new TransactionException(e);
		}
	}

	public interface DataTransfer {

		/**
		 * @param src
		 *          - where data currently lives.
		 * @param remoteTemp
		 *          - temporary which will be renamed/moved to the "to" location.
		 * @param dst
		 *          - the final destination. This URI is what will identify data
		 *          when getting the data back.
		 * 
		 *          Note: File systems that support renaming/directory moving
		 *          operations should transfer the data to the tempRemote.
		 * @throws IOException
		 */
		void transferData(URI src, URI remoteTemp, URI dst) throws IOException;

	}

}
