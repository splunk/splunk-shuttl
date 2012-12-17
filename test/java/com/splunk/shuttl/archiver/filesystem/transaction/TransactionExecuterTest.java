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

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class TransactionExecuterTest {

	private TransactionExecuter transactionExecuter;
	private Transaction transaction;

	@BeforeMethod
	public void setUp() {
		transaction = mock(Transaction.class);
		transactionExecuter = new TransactionExecuter();
	}

	public void execute_givenTransactionalTransfer_executesPrepareCommitAndCleanInOrder() {
		transactionExecuter.execute(transaction);

		InOrder inOrder = inOrder(transaction);
		inOrder.verify(transaction).prepare();
		inOrder.verify(transaction).commit();
		inOrder.verify(transaction).clean();
		inOrder.verifyNoMoreInteractions();
	}

	public void execute_prepareThrows_cleanButDontCommit() {
		doThrow(new RuntimeException()).when(transaction).prepare();
		try {
			transactionExecuter.execute(transaction);
			fail();
		} catch (RuntimeException e) {
		}
		verify(transaction).clean();
		verify(transaction, never()).commit();
	}

	public void execute_commitThrows_clean() {
		doThrow(new RuntimeException()).when(transaction).commit();
		try {
			transactionExecuter.execute(transaction);
			fail();
		} catch (RuntimeException e) {
		}
		verify(transaction).clean();
	}
}
