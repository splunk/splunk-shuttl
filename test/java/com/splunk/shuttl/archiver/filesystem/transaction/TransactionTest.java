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
import java.net.URI;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class TransactionTest {

	private Transaction transaction;
	private HasFileStructure hasFileStructure;
	private URI temp;
	private DataTransferer dataTransferer;
	private URI dst;
	private URI src;
	private Transaction bucketTransaction;
	private Bucket bucket;
	private TransactionCleaner transactionCleaner;

	@BeforeMethod
	public void setUp() {
		hasFileStructure = mock(HasFileStructure.class);
		dataTransferer = mock(DataTransferer.class);
		transactionCleaner = mock(TransactionCleaner.class);
		src = URI.create("local://file");
		temp = URI.create("remote://temp");
		dst = URI.create("remote://to");
		bucket = mock(Bucket.class);
		transaction = new Transaction(hasFileStructure, dataTransferer,
				transactionCleaner, src, temp, dst);
		bucketTransaction = new Transaction(hasFileStructure, dataTransferer,
				transactionCleaner, bucket, temp, dst);
	}

	public void prepare__createsDirsThenTransfersData() throws IOException {
		transaction.prepare();
		InOrder inOrder = inOrder(hasFileStructure, dataTransferer);
		inOrder.verify(hasFileStructure).mkdirs(temp);
		inOrder.verify(dataTransferer).transferData(src, temp, dst);
		inOrder.verifyNoMoreInteractions();
	}

	public void commit__renamesTempToTheRealDataPath() throws IOException {
		transaction.commit();
		verify(hasFileStructure).rename(temp, dst);
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void prepare_mkdirsThrowsException_throwsAndDoesNotTransferData()
			throws IOException {
		doThrow(IOException.class).when(hasFileStructure).mkdirs(any(URI.class));
		transaction.prepare();
		verifyZeroInteractions(dataTransferer);
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void prepare_dataTransferThrowsException_throwsTransactionException()
			throws IOException {
		doThrow(IOException.class).when(dataTransferer).transferData(any(URI.class),
				any(URI.class), any(URI.class));
		transaction.prepare();
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void commit_gotException_throws() throws IOException {
		doThrow(IOException.class).when(hasFileStructure).rename(any(URI.class),
				any(URI.class));
		transaction.commit();
	}

	public void prepare_constructedWithBucket_callsTransferBucket()
			throws IOException {
		bucketTransaction.prepare();
		verify(dataTransferer).transferBucket(bucket, temp, dst);
		verify(dataTransferer, never()).transferData(any(URI.class), any(URI.class),
				any(URI.class));
	}

	public void clean_constructedWithBucket_callsCleanBucketTransfer() {
		bucketTransaction.clean();
		verify(transactionCleaner).cleanBucketTransaction(bucket, temp);
	}

	public void clean_constructedWithURI_callsCleanBucketTransfer() {
		transaction.clean();
		verify(transactionCleaner).cleanFileTransaction(src, temp);
	}

}
