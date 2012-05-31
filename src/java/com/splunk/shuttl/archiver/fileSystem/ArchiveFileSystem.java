// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

/**
 * With this interface code can put, retrieve and list files in any system that
 * is used for to archiving.
 * 
 */
public interface ArchiveFileSystem {

	/**
	 * Puts the specified file on local file system to the archiving file system.
	 * 
	 * @param fileOnLocalFileSystem
	 *          An existing file on the local file system.
	 * @param fileOnArchiveFileSystem
	 *          Path pointing for an non exiting file on the archive file system.
	 * 
	 * @throws FileNotFoundException
	 *           If specified file on the local file system doesn't exist.
	 * @throws FileOverwriteException
	 *           If there is already a file on the specified path.
	 * @throws IOException
	 *           If there was any other problem with the operation.
	 */
	void putFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
			throws FileNotFoundException, FileOverwriteException, IOException;

	/**
	 * Puts the specified file on local file system to the archiving file system
	 * atomically. This method will first copy the specified files to a temporary
	 * place and then rename it to correct path.
	 * 
	 * @param fileOnLocalFileSystem
	 *          An existing file on the local file system.
	 * @param fileOnArchiveFileSystem
	 *          Path pointing for an non exiting file on the archive file system.
	 * 
	 * @throws FileNotFoundException
	 *           If specified file on the local file system doesn't exist.
	 * @throws FileOverwriteException
	 *           If there is already a file on the specified path.
	 * @throws IOException
	 *           If there was any other problem with the operation.
	 */
	void putFileAtomically(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
			throws FileNotFoundException, FileOverwriteException, IOException;

	/**
	 * Retrieves the file from specified path on archiving file system and stores
	 * it to the specified file on local file system.
	 * 
	 * @param fileOnLocalFileSystem
	 *          A non exiting file on the local file system.
	 * @param fileOnArchiveFileSystem
	 *          A path to an existing file on the archiving file system.
	 * @throws FileNotFoundException
	 *           If there isn't a file on the archiving file system with the
	 *           specified path.
	 * @throws FileOverwriteException
	 *           If there is already a file on the local file system.
	 * @throws IOException
	 *           If there was any other problem with the operation.
	 */
	void getFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
			throws FileNotFoundException, FileOverwriteException, IOException;

	/**
	 * Lists the contents of the specified path.
	 * 
	 * @param pathToBeListed
	 *          The returned list fill contains the contents of this path.
	 * @return One of three possibilities: 1. The contents of specified path. 2. A
	 *         list with only the path it self if its a file. 3. An empty list if
	 *         the specified path doesn't exist OR it's an empty directory.
	 * @throws IOException
	 *           If there was any other problem with the operation.
	 */
	List<URI> listPath(URI pathToBeListed) throws IOException;

	/**
	 * Returns the size of the specified URI (file or directory) in bytes.
	 * 
	 * @param uri
	 *          URI of file or directory on the file system
	 * @return null if the path specified by the URI does not exist - otherwise
	 *         the size of the file or directory in bytes
	 * @throws IOException
	 *           If there was any other problem with this operation
	 */
	Long getSize(URI uri) throws IOException;

	/**
	 * @param fileOnArchiveFileSystem
	 *          A path to an existing file on the archiving file system.
	 * @return an open {@link InputStream} to a file on the archive file system.
	 * @throws IOException
	 *           If there was any other problem with the operation.
	 */
	InputStream openFile(URI fileOnArchiveFileSystem) throws IOException;
}
