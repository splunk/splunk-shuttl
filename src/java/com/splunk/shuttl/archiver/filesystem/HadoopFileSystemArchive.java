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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splunk.shuttl.archiver.util.UtilsPath;

public class HadoopFileSystemArchive implements ArchiveFileSystem {

	private final Path atomicPutTmpPath;
	private final FileSystem hadoopFileSystem;

	public HadoopFileSystemArchive(FileSystem hadoopFileSystem, Path path) {
		this.hadoopFileSystem = hadoopFileSystem;
		this.atomicPutTmpPath = path;
	}

	@Override
	public void putFile(File file, URI remoteUri) throws FileNotFoundException,
			FileOverwriteException, IOException {
		throwIfFileDoNotExist(file);
		Path hadoopPath = createPath(remoteUri);
		throwIfRemotePathAlreadyExist(hadoopPath);

		hadoopFileSystem.copyFromLocalFile(createPath(file), hadoopPath);
	}

	@Override
	public void putFileAtomically(File file, URI remoteUri)
			throws FileNotFoundException, FileOverwriteException, IOException {
		Path hadoopPath = createPath(remoteUri);
		throwIfRemotePathAlreadyExist(hadoopPath);
		Path tmpLocation = putFileToTmpOverwritingOldFiles(file, remoteUri);
		move(tmpLocation, hadoopPath);
	}

	/**
	 * Do NOT call nor override this method outside this class.It's meant to be
	 * private but is package private for testing purposes. If you want to expose
	 * this method make it public or protected!
	 */
	/* package private */void deletePathRecursivly(Path fileOnArchiveFileSystem)
			throws IOException {
		hadoopFileSystem.delete(fileOnArchiveFileSystem, true);
	}

	/**
	 * Do NOT call nor override this method outside this class.It's meant to be
	 * private but is package private for testing purposes. If you want to expose
	 * this method make it public or protected!
	 */
	/* package private */void move(Path src, Path dst) throws IOException {
		hadoopFileSystem.mkdirs(dst.getParent());
		hadoopFileSystem.rename(src, dst);

	}

	/**
	 * Do NOT call nor override this method outside this class.It's meant to be
	 * private but is package private for testing purposes. If you want to expose
	 * this method make it public or protected!
	 * 
	 * The specified file will be copied from local file system in to the tmp
	 * directory on hadoop. The tmp directory will be the base and the full path
	 * of the file on hadoop will contains the specified URI.
	 */
	/* package private */Path putFileToTmpOverwritingOldFiles(File file,
			URI remoteUri) throws FileNotFoundException, IOException {
		Path hadoopPath = UtilsPath.createPathByAppending(atomicPutTmpPath,
				createPath(remoteUri));
		deletePathRecursivly(hadoopPath);
		try {
			putFile(file, hadoopPath.toUri());
		} catch (FileOverwriteException e) {
			throw new IOException("The old tmp path was not "
					+ "deleted. This should not happen!", e);
		}
		return hadoopPath;
	}

	@Override
	public void getFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
			throws FileNotFoundException, FileOverwriteException, IOException {
		throwExceptionIfFileAlreadyExist(fileOnLocalFileSystem);
		Path localPath = createPath(fileOnLocalFileSystem);
		Path hadoopPath = createPath(fileOnArchiveFileSystem);
		// FileNotFoundException is already thrown by copyToLocalFile.
		hadoopFileSystem.copyToLocalFile(hadoopPath, localPath);
	}

	@Override
	public List<URI> listPath(URI pathToBeListed) throws IOException {
		Path hadoopPath = createPath(pathToBeListed);
		FileStatus[] fileStatusOfPath = hadoopFileSystem.listStatus(hadoopPath);
		if (fileStatusOfPath != null)
			return new FileStatusBackedList(fileStatusOfPath);
		else
			return Collections.emptyList();
	}

	private Path createPath(URI uri) {
		return new Path(uri);
	}

	private Path createPath(File file) {
		return createPath(file.toURI());
	}

	private void throwIfFileDoNotExist(File file) throws FileNotFoundException {
		if (!file.exists())
			throw new FileNotFoundException(file.toString() + " doesn't exist.");
	}

	private void throwExceptionIfFileAlreadyExist(File file)
			throws FileOverwriteException {
		if (file.exists())
			throw new FileOverwriteException(file.toString() + " already exist.");
	}

	private void throwIfRemotePathAlreadyExist(Path path) throws IOException {
		if (hadoopFileSystem.exists(path))
			throw new FileOverwriteException(path.toString() + " already exist.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem#openFile(java.net
	 * .URI)
	 */
	@Override
	public InputStream openFile(URI fileOnArchiveFileSystem) throws IOException {
		return hadoopFileSystem.open(new Path(fileOnArchiveFileSystem));
	}
}
