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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.util.UtilsURI;

/**
 * Logic for helping with putting data or getting data. Calls appropriate
 * abstract methods for overriding.
 */
public class DataTransferer {

	private final TransfersFiles fileTransferer;
	private TransfersBuckets bucketTransferer;

	public DataTransferer(TransfersFiles fileTransferer,
			TransfersBuckets bucketTransferer) {
		this.fileTransferer = fileTransferer;
		this.bucketTransferer = bucketTransferer;
	}

	public void transferData(URI src, URI remoteTemp, URI dst) throws IOException {
		throwIfSchemesDiffer(remoteTemp, dst);
		if (isLocal(src))
			fileTransferer.putFile(new File(src), remoteTemp, dst);
		else
			fileTransferer.getFile(src, new File(remoteTemp), new File(dst));
	}

	private boolean isLocal(URI src) {
		return UtilsURI.isLocal(src);
	}

	public void transferBucket(Bucket src, URI remoteTemp, URI dst)
			throws IOException {
		throwIfSchemesDiffer(remoteTemp, dst);
		if (src.isRemote())
			bucketTransferer.getBucket(src, new File(remoteTemp), new File(dst));
		else
			bucketTransferer.putBucket(src, remoteTemp, dst);
	}

	private void throwIfSchemesDiffer(URI remoteTemp, URI dst) {
		if (schemesDiffer(remoteTemp, dst))
			throw new TransactionException(
					"Remote temp URI and destination URI must be the same. Temp: "
							+ remoteTemp + ", Destination: " + dst);
	}

	private boolean schemesDiffer(URI remoteTemp, URI dst) {
		return !remoteTemp.getScheme().equals(dst.getScheme());
	}

}
