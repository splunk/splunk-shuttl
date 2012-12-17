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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class AbstractTransactionTest {

	private Transaction transaction;
	private HasFileStructure hasFileStructure;
	private TransactionCleaner<String> transactionCleaner;
	private TransfersData<String> transfersData;

	private String data;
	private String temp;
	private String dst;

	@SuppressWarnings("unchecked")
	@BeforeMethod
	public void setUp() {
		hasFileStructure = mock(HasFileStructure.class);
		transactionCleaner = (TransactionCleaner<String>) mock(TransactionCleaner.class);
		transfersData = (TransfersData<String>) mock(TransfersData.class);
		data = "data";
		temp = "/remote/temp";
		dst = "/remote/dst";

		transaction = new AbstractTransaction<String>(hasFileStructure,
				transactionCleaner, data, temp, dst) {
			@Override
			protected void doTransferData(String data, String temp, String dst)
					throws IOException {
				transfersData.put(data, temp, dst);
			}
		};
	}

	public void prepare_destinationDoesNotExist_createsDirsThenTransfersData()
			throws IOException {
		when(hasFileStructure.exists(dst)).thenReturn(false);
		transaction.prepare();
		InOrder inOrder = inOrder(hasFileStructure, transfersData);
		inOrder.verify(hasFileStructure).mkdirs(temp);
		inOrder.verify(transfersData).put(data, temp, dst);
		inOrder.verifyNoMoreInteractions();
	}

	public void prepare_destinationExists_noMoreInteractions() throws IOException {
		when(hasFileStructure.exists(dst)).thenReturn(true);
		transaction.prepare();
		verify(hasFileStructure).exists(dst);
		verifyNoMoreInteractions(hasFileStructure, transfersData);
	}

	public void commit_destinationDoesNotExist_renamesTempToTheRealDataPath()
			throws IOException {
		when(hasFileStructure.exists(dst)).thenReturn(false);
		transaction.commit();
		verify(hasFileStructure).rename(temp, dst);
	}

	public void commit_destinationExists_doesNotCommit() throws IOException {
		when(hasFileStructure.exists(dst)).thenReturn(true);
		transaction.commit();
		verify(hasFileStructure).exists(dst);
		verifyNoMoreInteractions(hasFileStructure);
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void prepare_mkdirsThrowsException_throwsAndDoesNotTransferData()
			throws IOException {
		doThrow(IOException.class).when(hasFileStructure).mkdirs(anyString());
		transaction.prepare();
		verifyZeroInteractions(transfersData);
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void prepare_dataTransferThrowsException_throwsTransactionException()
			throws IOException {
		doThrow(IOException.class).when(transfersData).put(anyString(),
				anyString(), anyString());
		transaction.prepare();
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void commit_gotException_throws() throws IOException {
		doThrow(IOException.class).when(hasFileStructure).rename(anyString(),
				anyString());
		transaction.commit();
	}
}
