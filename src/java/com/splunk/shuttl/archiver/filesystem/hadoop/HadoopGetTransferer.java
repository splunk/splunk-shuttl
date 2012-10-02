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

import com.splunk.shuttl.archiver.filesystem.transaction.AbstractTransaction.DataTransfer;
import com.splunk.shuttl.archiver.filesystem.UnsupportedUriException;
import com.splunk.shuttl.archiver.util.UtilsURI;

/**
 * Gets files from Hadoop to local file system.
 */
public class HadoopGetTransferer implements DataTransfer {

	private final FileSystem fs;

	public HadoopGetTransferer(FileSystem fs) {
		this.fs = fs;
	}

	@Override
	public void transferData(URI src, URI remoteTemp, URI dst) throws IOException {
		throwWhenNotLocal(remoteTemp);
		throwWhenNotLocal(dst);
		fs.copyToLocalFile(new Path(src), new Path(remoteTemp));
	}

	private void throwWhenNotLocal(URI uri) {
		if (!UtilsURI.isLocal(uri))
			throw new UnsupportedUriException(uri
					+ " needs to be a local destination.");
	}
}
