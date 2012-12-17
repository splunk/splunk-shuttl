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

import org.apache.log4j.Logger;

/**
 * Executes {@link Transactions}
 */
public class TransactionExecuter {

	private static final Logger logger = Logger
			.getLogger(TransactionExecuter.class);

	/**
	 * Execute a transaction in the right order. Makes sure that clean is always
	 * called last, even if any other step throws exception.
	 */
	public void execute(Transaction transaction) {
		TransactionExecuter.executeTransaction(transaction);
	}

	/**
	 * Execute a transaction in the right order. Makes sure that clean is always
	 * called last, even if any other step throws exception.
	 */
	public static void executeTransaction(Transaction transaction) {
		try {
			logger.info(will("Prepare transaction", "transaction", transaction));
			transaction.prepare();
			logger.info(done("Preparing transaction", "transaction", transaction));
			logger.info(will("Commit transaction", "transaction", transaction));
			transaction.commit();
			logger.info(done("Commit transaction", "transaction", transaction));
		} catch (RuntimeException e) {
			logger.error(did("Executed transaction", e,
					"Transaction to prepare and commit", "transaction", transaction));
			throw e;
		} finally {
			transaction.clean();
		}
	}
}
