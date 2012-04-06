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
package com.splunk.shep.archiver.listers;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;

/**
 * Lists indexes in an {@link ArchiveFileSystem}
 */
public class ArchivedIndexesLister {

    private final static Logger logger = Logger
	    .getLogger(ArchivedIndexesLister.class);
    private final PathResolver pathResolver;
    private final ArchiveFileSystem fileSystem;

    /**
     * @param pathResolver
     *            used to resolve paths on {@link ArchiveFileSystem}
     * @param fileSystem
     *            {@link ArchiveFileSystem} to list indexes on
     */
    public ArchivedIndexesLister(PathResolver pathResolver,
	    ArchiveFileSystem fileSystem) {
	this.pathResolver = pathResolver;
	this.fileSystem = fileSystem;
    }

    /**
     * @return {@link List} of indexes that are archived in a
     *         {@link ArchiveFileSystem}
     */
    public List<String> listIndexes() {
	URI indexesHome = pathResolver.getIndexesHome();
	List<URI> indexUris = listIndexesUrisOnArchiveFileSystem(indexesHome);
	List<String> indexes = new ArrayList<String>();
	for (URI uri : indexUris) {
	    indexes.add(FilenameUtils.getName(uri.getPath()));
	}
	return indexes;
    }

    private List<URI> listIndexesUrisOnArchiveFileSystem(URI indexesHome) {
	try {
	    return fileSystem.listPath(indexesHome);
	} catch (IOException e) {
	    logger.debug(did("Listed indexes at indexesHome",
		    "Got IOException",
		    "To list indexes on the archive filesystem",
		    "indexes_home", indexesHome, "exception", e));
	    throw new RuntimeException(e);
	}
    }
}
