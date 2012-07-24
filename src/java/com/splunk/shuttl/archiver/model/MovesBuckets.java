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
package com.splunk.shuttl.archiver.model;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.log4j.Logger;


/**
 * Moves a Bucket to a destination
 */
public class MovesBuckets {

	private static final Logger logger = Logger.getLogger(MovesBuckets.class);

	/**
	 * @param destinationDirectory
	 *          to move the bucket to.
	 * @return the new moved bucket.
	 * @throws FileNotFoundException
	 *           if the destination directory does not exist
	 * @throws FileNotDirectoryException
	 *           when the destination is not a directory.
	 */
	public static Bucket moveBucket(Bucket bucket, File destinationDirectory) {
		verifyValidityOfDestination(destinationDirectory);
		logger.debug(will("Attempting to move bucket", "bucket", bucket,
				"destination", destinationDirectory));
		File originDirectory = bucket.getDirectory();
		File newDirectory = new File(destinationDirectory,
				originDirectory.getName());
		if (!originDirectory.renameTo(newDirectory))
			logMoveFailureAndThrowException(bucket, destinationDirectory);
		return BucketFactory.createBucketWithIndexAndDirectory(bucket.getIndex(),
				newDirectory);
	}

	private static void verifyValidityOfDestination(File destinationDirectory) {
		if (!destinationDirectory.exists())
			throw new DirectoryDidNotExistException("Cannot move bucket to: "
					+ destinationDirectory.getAbsolutePath()
					+ ", because directory did not exist.");
		if (!destinationDirectory.isDirectory())
			throw new FileNotDirectoryException("Cannot move bucket to: "
					+ destinationDirectory.getAbsolutePath()
					+ ", because it's not a directory");
	}

	private static void logMoveFailureAndThrowException(Bucket bucket,
			File destinationDirectory) {
		logger.error(did("Attempted to move bucket", "move failed", null, "bucket",
				bucket, "destination", destinationDirectory));
		throw new RuntimeException("Couldn't move bucket to destination: "
				+ destinationDirectory);
	}

}
