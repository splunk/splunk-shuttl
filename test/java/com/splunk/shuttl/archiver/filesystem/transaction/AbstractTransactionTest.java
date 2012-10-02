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

import com.splunk.shuttl.archiver.filesystem.transaction.AbstractTransaction.DataTransfer;

@Test(groups = { "fast-unit" })
public class AbstractTransactionTest {

	private AbstractTransaction transferer;
	private HasFileStructure hasFileStructure;
	private URI temp;
	private DataTransfer dataTransfer;
	private URI to;
	private URI local;

	@BeforeMethod
	public void setUp() {
		hasFileStructure = mock(HasFileStructure.class);
		dataTransfer = mock(DataTransfer.class);
		local = URI.create("local://file");
		temp = URI.create("remote://temp");
		to = URI.create("remote://to");
		transferer = new AbstractTransaction(hasFileStructure, dataTransfer, local,
				temp, to) {

			@Override
			public void clean() {
				// do nothing
			}
		};

	}

	public void prepare__createsDirsThenTransfersData() throws IOException {
		transferer.prepare();
		InOrder inOrder = inOrder(hasFileStructure, dataTransfer);
		inOrder.verify(hasFileStructure).mkdirs(temp);
		inOrder.verify(dataTransfer).transferData(local, temp, to);
		inOrder.verifyNoMoreInteractions();
	}

	public void commit__renamesTempToTheRealDataPath() throws IOException {
		transferer.commit();
		verify(hasFileStructure).rename(temp, to);
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void prepare_mkdirsThrowsException_throwsAndDoesNotTransferData()
			throws IOException {
		doThrow(IOException.class).when(hasFileStructure).mkdirs(any(URI.class));
		transferer.prepare();
		verifyZeroInteractions(dataTransfer);
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void prepare_dataTransferThrowsException_throwsTransactionException()
			throws IOException {
		doThrow(IOException.class).when(dataTransfer).transferData(any(URI.class),
				any(URI.class), any(URI.class));
		transferer.prepare();
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void commit_gotException_throws() throws IOException {
		doThrow(IOException.class).when(hasFileStructure).rename(any(URI.class),
				any(URI.class));
		transferer.commit();
	}

}
