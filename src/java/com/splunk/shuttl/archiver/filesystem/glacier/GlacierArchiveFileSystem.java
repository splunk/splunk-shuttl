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
package com.splunk.shuttl.archiver.filesystem.glacier;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * The glacier file system is not good for storing multiple files, it will
 * therefore rely on s3 to handle the storing of meta data and file structure.
 * It supports only buckets that contain a single file.
 */
public class GlacierArchiveFileSystem implements ArchiveFileSystem {

	private final HadoopArchiveFileSystem hadoop;
	private final String id;
	private final String secret;
	private final String endpoint;
	private final String vault;

	public GlacierArchiveFileSystem(HadoopArchiveFileSystem hadoop, String id,
			String secret, String endpoint, String vault) {
		this.hadoop = hadoop;
		this.id = id;
		this.secret = secret;
		this.endpoint = endpoint;
		this.vault = vault;
	}

	@Override
	public void putBucket(Bucket localBucket, URI temp, URI dst)
			throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void getBucket(Bucket remoteBucket, File temp, File dst)
			throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void cleanBucketTransaction(Bucket bucket, URI temp) {
		// Do nothing.
	}

	public String getId() {
		return id;
	}

	public String getSecret() {
		return secret;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getVault() {
		return vault;
	}

	@Override
	public void putFile(File src, URI temp, URI dst) throws IOException {
		hadoop.putFile(src, temp, dst);
	}

	@Override
	public void getFile(URI src, File temp, File dst) throws IOException {
		hadoop.getFile(src, temp, dst);
	}

	@Override
	public void mkdirs(URI uri) throws IOException {
		hadoop.mkdirs(uri);
	}

	@Override
	public void rename(URI from, URI to) throws IOException {
		hadoop.rename(from, to);
	}

	@Override
	public void cleanFileTransaction(URI src, URI temp) {
		hadoop.cleanFileTransaction(src, temp);
	}

	@Override
	public List<URI> listPath(URI pathToBeListed) throws IOException {
		return hadoop.listPath(pathToBeListed);
	}

	@Override
	public InputStream openFile(URI fileOnArchiveFileSystem) throws IOException {
		return hadoop.openFile(fileOnArchiveFileSystem);
	}

}
