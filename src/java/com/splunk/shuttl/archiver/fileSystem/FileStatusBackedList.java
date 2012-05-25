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

import java.net.URI;
import java.util.AbstractList;

import org.apache.hadoop.fs.FileStatus;

/**
 * Wrapes around {@link FileStatus} and provides a list of URI objects.
 * 
 */
public class FileStatusBackedList extends AbstractList<URI> {

    private FileStatus[] fileStatus;
    private URI[] uriCache;

    /**
     * Creates a list backed by the specified FileStatus array.
     */
    public FileStatusBackedList(FileStatus... fileStatus) {
	super();
	this.fileStatus = fileStatus;
	uriCache = new URI[fileStatus.length];
    }

    @Override
    public URI get(int index) {
	if (uriCache[index] == null) {
	    uriCache[index] = fileStatus[index].getPath().toUri();
	}
	return uriCache[index];
    }

    @Override
    public int size() {
	return fileStatus.length;
    }

}
