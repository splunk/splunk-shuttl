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

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Provides the generic method calls for doing a TransactionalTransfer.
 */
public class Transaction {

	private static final Logger logger = Logger.getLogger(Transaction.class);

	private final HasFileStructure hasFileStructure;
	private final DataTransferer dataTransferer;
	private final TransactionCleaner transactionCleaner;
	private final Bucket bucket;
	private final URI src;
	private final URI remoteTemp;
	private final URI dst;

	protected Transaction(TransactionalFileSystem reciever,
			DataTransferer dataTransferer, URI src, URI remoteTemp, URI dst) {
		this(reciever, dataTransferer, reciever, null, src, remoteTemp, dst);
	}

	protected Transaction(TransactionalFileSystem reciever,
			DataTransferer dataTransferer, Bucket bucket, URI remoteTemp, URI dst) {
		this(reciever, dataTransferer, reciever, bucket, bucket.getURI(),
				remoteTemp, dst);
	}

	private Transaction(HasFileStructure hasFileStructure,
			DataTransferer dataTransfer, TransactionCleaner transactionCleaner,
			Bucket bucket, URI src, URI remoteTemp, URI dst) {
		this.hasFileStructure = hasFileStructure;
		this.dataTransferer = dataTransfer;
		this.transactionCleaner = transactionCleaner;
		this.bucket = bucket;
		this.src = src;
		this.remoteTemp = remoteTemp;
		this.dst = dst;
	}

	/**
	 * Transfer the data invisible from the rest of the system. This must be
	 * blocking until the whole preparation is done.
	 */
	public void prepare() {
		makeDirectories();
		transferData();
	}

	private void makeDirectories() {
		try {
			hasFileStructure.mkdirs(remoteTemp);
		} catch (Exception e) {
			logger.error(did("Tried making directories up to: " + remoteTemp, e,
					"To make directories.", "uri", remoteTemp));
			throw new TransactionException(e);
		}
	}

	private void transferData() {
		try {
			transferBucketOrData();
		} catch (IOException e) {
			throwAndLog(e);
		}
	}

	private void transferBucketOrData() throws IOException {
		if (hasBucket())
			dataTransferer.transferBucket(bucket, remoteTemp, dst);
		else
			dataTransferer.transferData(src, remoteTemp, dst);
	}

	private boolean hasBucket() {
		return bucket != null;
	}

	private void throwAndLog(IOException e) {
		logger.error(did("Transferred data to DataTransferer: " + dataTransferer,
				e, "To transfer file to remote file system via temp.", "from", src,
				"to", dst, "temp", remoteTemp));
		throw new TransactionException(e);
	}

	/**
	 * Make an atomic operation that commits the data to the system. This will
	 * happen when prepare is done.
	 */
	public void commit() {
		try {
			hasFileStructure.rename(remoteTemp, dst);
		} catch (IOException e) {
			logger
					.error(did("Tried commiting transaction", e,
							"To complete transaction", "from", src, "to", dst, "temp",
							remoteTemp));
			throw new TransactionException(e);
		}
	}

	/**
	 * Clean any data that litters the system. Will be called whatever happens in
	 * the prepare or commit stages.
	 */
	public void clean() {
		if (hasBucket())
			transactionCleaner.cleanBucketTransaction(bucket, remoteTemp);
		else
			transactionCleaner.cleanFileTransaction(src, remoteTemp);
	}

	/**
	 * Generated ->
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Transaction other = (Transaction) obj;
		if (bucket == null) {
			if (other.bucket != null)
				return false;
		} else if (!bucket.equals(other.bucket))
			return false;
		if (dst == null) {
			if (other.dst != null)
				return false;
		} else if (!dst.equals(other.dst))
			return false;
		if (remoteTemp == null) {
			if (other.remoteTemp != null)
				return false;
		} else if (!remoteTemp.equals(other.remoteTemp))
			return false;
		if (src == null) {
			if (other.src != null)
				return false;
		} else if (!src.equals(other.src))
			return false;
		return true;
	}

	public Bucket getBucket() {
		return bucket;
	}

	public URI getSrc() {
		return src;
	}

	public URI getRemoteTemp() {
		return remoteTemp;
	}

	public URI getDst() {
		return dst;
	}
}
