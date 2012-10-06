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
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.HashMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.splunk.shuttl.archiver.util.UtilsURI;

/**
 * Implementation of doing operations to the Amazon Glacier service.
 */
public class GlacierClient {

	private ArchiveTransferManager transferManager;
	private String vault;
	private HashMap<URI, String> archiveIds;

	/**
	 * @param transferManager
	 * @param vault
	 */
	public GlacierClient(ArchiveTransferManager transferManager, String vault) {
		this.transferManager = transferManager;
		this.vault = vault;
		this.archiveIds = new HashMap<URI, String>();
	}

	/**
	 * Uploads a file to glacier and stores the of the transfer archiveId in
	 * memory.
	 */
	public void upload(File file, URI dst) throws AmazonServiceException,
			AmazonClientException, FileNotFoundException {
		UploadResult result = transferManager.upload(vault, dst.toString(), file);
		putArchiveId(dst, result.getArchiveId());
	}

	/**
	 * Downloads a file stored in glacier with a URI.
	 * 
	 * @throws GlacierArchiveIdDoesNotExist
	 *           if the archiveId is not stored in memory.
	 */
	public void downloadToDir(URI uri, File dir) {
		if (!dir.exists())
			dir.mkdirs();
		if (!dir.isDirectory())
			throw new IllegalArgumentException("File needs to be a directory: " + dir);

		String filename = UtilsURI.getFileNameWithTrimmedEndingFileSeparator(uri);
		transferManager.download(vault, getArchiveId(uri), new File(dir, filename));
	}

	/**
	 * Get the archiveId mapped to a URI.
	 */
	public String getArchiveId(URI uri) {
		if (!archiveIds.containsKey(uri))
			throw new GlacierArchiveIdDoesNotExist(
					"Could not get the archiveId for uri: " + uri
							+ ", which means that we cannot download the archive. "
							+ "Download the archive inventory and parse out the "
							+ "description for the uri->archiveId mappings.");
		return archiveIds.get(uri);
	}

	/**
	 * Map a uri to a archiveId.
	 */
	public void putArchiveId(URI uri, String archiveId) {
		archiveIds.put(uri, archiveId);
	}

	public static GlacierClient create(AWSCredentialsImpl credentials) {
		AmazonGlacierClient amazonGlacierClient = new AmazonGlacierClient(
				credentials);
		amazonGlacierClient.setEndpoint(credentials.getEndpoint());
		return new GlacierClient(new ArchiveTransferManager(amazonGlacierClient,
				credentials), credentials.getVault());
	}
}
