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
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;

/**
 * Fake implementation of the {@link ArchiveTransferManager} that "downloads"
 * and "uploads" files to the local file system, instead of the real Glacier
 * file system.
 */
public class FakeArchiveTransferManager extends ArchiveTransferManager {

	private final File managedDirectory;
	private final HashMap<String, File> files;

	public FakeArchiveTransferManager(File directory) {
		super(null);
		this.managedDirectory = directory;
		this.files = new HashMap<String, File>();
	}

	@Override
	public UploadResult upload(String vaultName, String archiveDescription,
			File file) throws AmazonServiceException, AmazonClientException,
			FileNotFoundException {
		return upload("fake acount id", vaultName, archiveDescription, file);
	}

	@Override
	public UploadResult upload(String accountId, String vaultName,
			String archiveDescription, File file) throws AmazonServiceException,
			AmazonClientException, FileNotFoundException {
		copyFileToManagedDirectory(file);
		UUID uuid = storeUuidForFile(file);
		return new UploadResult(uuid.toString());
	}

	private void copyFileToManagedDirectory(File file) {
		copyFile(file, fileInManagedDirectory(file));
	}

	private void copyFile(File managedFile, File outputFile) {
		try {
			FileUtils.copyFile(managedFile, outputFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private File fileInManagedDirectory(File file) {
		return new File(managedDirectory, file.getName());
	}

	private UUID storeUuidForFile(File file) {
		UUID uuid = UUID.randomUUID();
		files.put(uuid.toString(), fileInManagedDirectory(file));
		return uuid;
	}

	@Override
	public void download(String arg0, String arg1, String arg2, File arg3)
			throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void download(String vaultName, String archiveId, File outputFile)
			throws AmazonServiceException, AmazonClientException {
		if (outputFile.exists())
			throw new RuntimeException("Cannot download file to an "
					+ "existing output file: " + outputFile);
		if (!files.containsKey(archiveId))
			throw new UnknownArchiveIdException();
		File managedFile = files.get(archiveId);
		copyFile(managedFile, outputFile);
	}

	/**
	 * Gets an uploaded file that is identified by its archiveId.
	 */
	public File getFile(String archiveId) {
		return files.get(archiveId);
	}

	public static class UnknownArchiveIdException extends RuntimeException {

		private static final long serialVersionUID = 1L;

	}
}
