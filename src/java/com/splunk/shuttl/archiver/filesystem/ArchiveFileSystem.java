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

package com.splunk.shuttl.archiver.filesystem;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import com.splunk.shuttl.archiver.filesystem.transaction.TransactionalFileSystem;

/**
 * With this interface code can put, retrieve and list files in any system that
 * is used for to archiving.
 * 
 */
public interface ArchiveFileSystem extends TransactionalFileSystem {

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
	 * @param fileOnArchiveFileSystem
	 *          A path to an existing file on the archiving file system.
	 * @return an open {@link InputStream} to a file on the archive file system.
	 * @throws IOException
	 *           If there was any other problem with the operation.
	 */
	InputStream openFile(URI fileOnArchiveFileSystem) throws IOException;
}
