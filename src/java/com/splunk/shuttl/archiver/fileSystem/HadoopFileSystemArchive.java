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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.util.UtilsPath;

public class HadoopFileSystemArchive implements ArchiveFileSystem {

	private final Path atomicPutTmpPath;
	private final FileSystem hadoopFileSystem;

	private static Logger logger = Logger
			.getLogger(HadoopFileSystemArchive.class);

	public HadoopFileSystemArchive(FileSystem hadoopFileSystem, Path path) {
		atomicPutTmpPath = path;
		this.hadoopFileSystem = hadoopFileSystem;
	}

	@Override
	public void putFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
			throws FileNotFoundException, FileOverwriteException, IOException {
		throwExceptionIfFileDoNotExist(fileOnLocalFileSystem);
		Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
		throwExceptionIfRemotePathAlreadyExist(hadoopPath);
		Path localPath = createPathFromFile(fileOnLocalFileSystem);
		hadoopFileSystem.copyFromLocalFile(localPath, hadoopPath);
	}

	@Override
	public void putFileAtomically(File fileOnLocalFileSystem,
			URI fileOnArchiveFileSystem) throws FileNotFoundException,
			FileOverwriteException, IOException {
		Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
		throwExceptionIfRemotePathAlreadyExist(hadoopPath);
		Path tmpLocation = putFileToTmpDirectoryOverwirtingOldFilesAppendingPath(
				fileOnLocalFileSystem, fileOnArchiveFileSystem);
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
	/* package private */Path putFileToTmpDirectoryOverwirtingOldFilesAppendingPath(
			File fileOnLocalFileSystem, URI appendPathToTmpDirectory)
			throws FileNotFoundException, IOException {

		Path hadoopPath = UtilsPath.createPathByAppending(atomicPutTmpPath,
				createPathFromURI(appendPathToTmpDirectory));
		deletePathRecursivly(hadoopPath);
		try {
			putFile(fileOnLocalFileSystem, hadoopPath.toUri());

		} catch (FileOverwriteException e) {
			throw new IOException(
					"The old tmp path was not deleted this shouldn't happen!", e);
		}
		return hadoopPath;
	}

	@Override
	public void getFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
			throws FileNotFoundException, FileOverwriteException, IOException {
		throwExceptionIfFileAlreadyExist(fileOnLocalFileSystem);
		Path localPath = createPathFromFile(fileOnLocalFileSystem);
		Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
		// FileNotFoundException is already thrown by copyToLocalFile.
		hadoopFileSystem.copyToLocalFile(hadoopPath, localPath);
	}

	@Override
	public List<URI> listPath(URI pathToBeListed) throws IOException {
		Path hadoopPath = createPathFromURI(pathToBeListed);
		FileStatus[] fileStatusOfPath = hadoopFileSystem.listStatus(hadoopPath);
		if (fileStatusOfPath != null)
			return new FileStatusBackedList(fileStatusOfPath);
		else
			return Collections.emptyList();
	}

	private Path createPathFromURI(URI uri) {
		return new Path(uri);
	}

	private Path createPathFromFile(File file) {
		return createPathFromURI(file.toURI());
	}

	private void throwExceptionIfFileDoNotExist(File file)
			throws FileNotFoundException {
		if (!file.exists())
			throw new FileNotFoundException(file.toString() + " doesn't exist.");
	}

	private void throwExceptionIfFileAlreadyExist(File file)
			throws FileOverwriteException {
		if (file.exists())
			throw new FileOverwriteException(file.toString() + " already exist.");
	}

	private void throwExceptionIfRemotePathAlreadyExist(Path path)
			throws IOException {
		if (hadoopFileSystem.exists(path))
			throw new FileOverwriteException(path.toString() + " already exist.");
	}

	/**
	 * There appears to be no method in the HDFS API that gives the size of a
	 * directory, so we perform a search to get accurate directory sizes.
	 */
	@Override
	public Long getSize(URI uri) throws IOException {
		// DFS for now
		FileStatus file = hadoopFileSystem.getFileStatus(createPathFromURI(uri));
		long size = file.getLen();
		if (file.isDir()) {
			Stack<FileStatus> files = new Stack<FileStatus>();
			files.add(file);
			while (!files.isEmpty()) {
				file = files.pop();
				size += file.getLen();
				if (file.isDir())
					files
							.addAll(Arrays.asList(hadoopFileSystem.listStatus(file.getPath())));
			}
		}
		return size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem#openFile(java.net
	 * .URI)
	 */
	@Override
	public InputStream openFile(URI fileOnArchiveFileSystem) throws IOException {
		return hadoopFileSystem.open(new Path(fileOnArchiveFileSystem));
	}
}