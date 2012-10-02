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
package com.splunk.shuttl.archiver.filesystem.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splunk.shuttl.archiver.filesystem.transaction.AbstractTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.AbstractTransaction.DataTransfer;
import com.splunk.shuttl.archiver.filesystem.FileOverwriteException;
import com.splunk.shuttl.archiver.filesystem.UnsupportedUriException;
import com.splunk.shuttl.archiver.util.UtilsURI;

/**
 * Transfers local file to a Hadoop compliant file system. Transfers to the
 * remoteTemp instead of the destination, since it'll be moved to the real
 * location when the transaction is done. @See {@link AbstractTransaction}
 */
public class HadoopPutTransferer implements DataTransfer {

	private final FileSystem fs;

	public HadoopPutTransferer(FileSystem fs) {
		this.fs = fs;
	}

	@Override
	public void transferData(URI src, URI remoteTemp, URI dst) throws IOException {
		throwWhenIsNotLocalFile(src);
		throwWhenExists(dst);

		Path temp = new Path(remoteTemp);
		fs.delete(temp, true);
		fs.copyFromLocalFile(new Path(src), temp);
	}

	private void throwWhenIsNotLocalFile(URI uri) {
		if (!UtilsURI.isLocal(uri))
			throw new UnsupportedUriException("Can only transfer files "
					+ "from local file system.");
	}

	private void throwWhenExists(URI uri) throws IOException,
			FileOverwriteException {
		if (fs.exists(new Path(uri)))
			throw new FileOverwriteException(uri.toString() + " already exists.");
	}

}
