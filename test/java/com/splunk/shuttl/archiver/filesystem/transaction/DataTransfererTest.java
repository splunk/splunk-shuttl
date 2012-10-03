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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class DataTransfererTest {

	private DataTransferer transferer;
	private URI local;
	private URI remote;
	private TransfersFiles transfersFiles;
	private TransfersBuckets transfersBuckets;

	@BeforeMethod
	public void setUp() {
		transfersFiles = mock(TransfersFiles.class);
		transfersBuckets = mock(TransfersBuckets.class);
		transferer = new DataTransferer(transfersFiles, transfersBuckets);
		local = createFile().toURI();
		remote = URI.create("remote://uri");
	}

	public void transferData_givenLocalRemoteRemote_callsPutFile()
			throws IOException {
		transferer.transferData(local, remote, remote);
		verify(transfersFiles).putFile(eq(new File(local)), eq(remote), eq(remote));
	}

	public void transferData_givenRemoteLocalLocal_callsGetFile()
			throws IOException {
		transferer.transferData(remote, local, local);
		verify(transfersFiles).getFile(eq(remote), eq(new File(local)),
				eq(new File(local)));
	}

	public void transferData_givenAllLocals_callsPutFile() throws IOException {
		transferer.transferData(local, local, local);
		verify(transfersFiles).putFile(eq(new File(local)), eq(local), eq(local));
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void transferData_remoteTempAndDstSchemesDiffer_throws()
			throws IOException {
		transferer.transferData(null, remote, local);
	}

	public void transferBucket_remoteBucket_getsBucket() throws IOException {
		Bucket bucket = TUtilsBucket.createRemoteBucket();
		transferer.transferBucket(bucket, local, local);
		verify(transfersBuckets).getBucket(eq(bucket), eq(new File(local)),
				eq(new File(local)));
	}

	public void transferBucket_localBucket_putsBucket() throws IOException {
		Bucket bucket = TUtilsBucket.createBucket();
		transferer.transferBucket(bucket, remote, remote);
		verify(transfersBuckets).putBucket(bucket, remote, remote);
	}

	@Test(expectedExceptions = { TransactionException.class })
	public void transferBucket_remoteTempAndDstSchemesDiffer_throws()
			throws IOException {
		transferer.transferBucket(null, remote, local);
	}

}
