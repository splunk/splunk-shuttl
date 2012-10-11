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

import org.apache.log4j.Logger;

/**
 * Provides the generic method calls for doing a TransactionalTransfer.
 */
public abstract class AbstractTransaction<T> implements Transaction {

	private static final Logger logger = Logger
			.getLogger(AbstractTransaction.class);

	private final HasFileStructure hasFileStructure;
	private final TransactionCleaner<T> transactionCleaner;
	private final T data;
	private final String temp;
	private final String dst;

	protected AbstractTransaction(HasFileStructure hasFileStructure,
			TransactionCleaner<T> transactionCleaner, T data, String temp, String dst) {
		this.hasFileStructure = hasFileStructure;
		this.transactionCleaner = transactionCleaner;
		this.data = data;
		this.temp = temp;
		this.dst = dst;
	}

	@Override
	public void prepare() {
		makeDirectories();
		transferData();
	}

	private void makeDirectories() {
		try {
			hasFileStructure.mkdirs(temp);
		} catch (Exception e) {
			logger.error(did("Tried making directories up to: " + temp, e,
					"To make directories.", "path", temp));
			throw new TransactionException(e);
		}
	}

	private void transferData() {
		try {
			doTransferData(data, temp, dst);
		} catch (IOException e) {
			throwAndLog(e);
		}
	}

	protected abstract void doTransferData(T data, String temp, String dst)
			throws IOException;

	private void throwAndLog(IOException e) {
		logger.error(did("Transferred data with transaction: " + this, e,
				"To transfer file to remote file system via temp.", "from", data, "to",
				dst, "temp", temp));
		throw new TransactionException(e);
	}

	@Override
	public void commit() {
		try {
			hasFileStructure.rename(temp, dst);
		} catch (IOException e) {
			logger.error(did("Tried commiting transaction", e,
					"To complete transaction", "from", data, "to", dst, "temp", temp));
			throw new TransactionException(e);
		}
	}

	@Override
	public void clean() {
		transactionCleaner.cleanTransaction(data, temp);
	}

	/**
	 * Generated ->
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractTransaction other = (AbstractTransaction) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (dst == null) {
			if (other.dst != null)
				return false;
		} else if (!dst.equals(other.dst))
			return false;
		if (temp == null) {
			if (other.temp != null)
				return false;
		} else if (!temp.equals(other.temp))
			return false;
		return true;
	}

	public T getData() {
		return data;
	}

	public String getTemp() {
		return temp;
	}

	public String getDst() {
		return dst;
	}

	@Override
	public String toString() {
		return "Transaction [data=" + data + ", remoteTemp=" + temp + ", dst="
				+ dst + "]";
	}
}
