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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;

/**
 * Implementation of doing operations to the Amazon Glacier service.
 */
public class GlacierClient {

	private static final Logger logger = Logger.getLogger(GlacierClient.class);

	private ArchiveTransferManager transferManager;
	private String vault;
	// TODO: Better solution with db or flat files?
	private static final HashMap<String, String> archiveIdCache = new HashMap<String, String>();
	private final Map<String, String> archiveIds;

	/**
	 * Uses the in-memory static final archiveIdCache.
	 */
	private GlacierClient(ArchiveTransferManager transferManager, String vault) {
		this(transferManager, vault, archiveIdCache);
	}

	public GlacierClient(ArchiveTransferManager transferManager, String vault,
			Map<String, String> archiveIds) {
		this.transferManager = transferManager;
		this.vault = vault;
		this.archiveIds = archiveIds;
	}

	/**
	 * Uploads a file to glacier and stores the of the transfer archiveId in
	 * memory.
	 */
	public void upload(File file, String dst) throws AmazonServiceException,
			AmazonClientException, FileNotFoundException {
		logger.info(will("Use amazon glacier ArchiveTransferManager"
				+ " to transfer file to a vault", "file", file, "vault", vault,
				"destination", dst));
		UploadResult result = transferManager.upload(vault, dst, file);
		logger.info(done("Uploading file to glacier."));
		putArchiveId(dst, result.getArchiveId());
	}

	/**
	 * Downloads a file stored in glacier with a path.
	 * 
	 * @throws GlacierArchiveIdDoesNotExist
	 *           if the archiveId is not stored in memory.
	 */
	public void downloadToDir(String src, File dir) {
		if (!dir.exists())
			dir.mkdirs();
		if (!dir.isDirectory())
			throw new IllegalArgumentException("File needs to be a directory: " + dir);

		doTheDownload(src, dir);
	}

	private void doTheDownload(String src, File dir) {
		String filename = FilenameUtils.getName(src) + ".csv";
		File destinationFile = new File(dir, filename);
		String archiveId = getArchiveId(src);

		logger.info(will("Download archive from glacier", "destination",
				destinationFile, "src", src, "to-dir", dir, "filename", filename,
				"archiveId", archiveId));
		transferManager.download(vault, archiveId, destinationFile);
		logger.info(done("Downloaded archive from glacier", "destination",
				destinationFile));
	}

	/**
	 * Get the archiveId mapped to a path.
	 */
	public String getArchiveId(String path) {
		if (!archiveIds.containsKey(path))
			throw new GlacierArchiveIdDoesNotExist(
					"Could not get the archiveId for dst: " + path
							+ ", which means that we cannot download the archive. "
							+ "Download the archive inventory and parse out the "
							+ "description for the path->archiveId mappings.");
		return archiveIds.get(path);
	}

	/**
	 * Map a path to a archiveId.
	 */
	public void putArchiveId(String path, String archiveId) {
		archiveIds.put(path, archiveId);
	}

	public static GlacierClient create(AWSCredentialsImpl credentials) {
		AmazonGlacierClient amazonGlacierClient = new AmazonGlacierClient(
				credentials);
		amazonGlacierClient.setEndpoint(credentials.getGlacierEndpoint());
		return new GlacierClient(new ArchiveTransferManager(amazonGlacierClient,
				credentials), credentials.getGlacierVault());
	}
}
