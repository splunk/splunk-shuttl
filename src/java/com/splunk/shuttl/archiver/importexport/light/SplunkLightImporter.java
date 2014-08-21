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
package com.splunk.shuttl.archiver.importexport.light;

import static java.util.Arrays.*;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LogFormatter;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.importexport.csv.splunk.SplunkEnvironment;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;

public class SplunkLightImporter implements BucketImporter {

	private static final Logger logger = Logger
			.getLogger(SplunkLightImporter.class);

	private final SplunkBucketRepairTool splunkRebuildTool;

	public SplunkLightImporter(SplunkBucketRepairTool splunkRebuildTool) {
		this.splunkRebuildTool = splunkRebuildTool;
	}

	@Override
	public LocalBucket importBucket(LocalBucket b) {
		LocalBucket rebuiltBucket = splunkRebuildTool.rebuild(b);
		return BucketFactory.createBucketWithIndexDirectoryAndFormat(b.getIndex(),
				rebuiltBucket.getDirectory(), BucketFormat.SPLUNK_BUCKET);
	}

	public static SplunkLightImporter create() {
		return new SplunkLightImporter(new SplunkBucketRepairTool(

		ShellExecutor.getInstance(), SplunkEnvironment.getSplunkHome(),
				SplunkEnvironment.getEnvironment()));
	}

	private static class SplunkBucketRepairTool {

		private final ShellExecutor shellExecutor;
		private final String splunkHome;
		private final Map<String, String> env;

		public SplunkBucketRepairTool(ShellExecutor shellExecutor,
				String splunkHome, Map<String, String> env) {
			this.shellExecutor = shellExecutor;
			this.splunkHome = splunkHome;
			this.env = env;
		}

		public LocalBucket rebuild(LocalBucket b) {
			String splunkBinary = new File(splunkHome, "bin" + File.separator
					+ "splunk").getAbsolutePath();
			List<String> command = asList(splunkBinary, "fsck", "repair",
					"--one-bucket", "--include-hots", "--bucket-path="
							+ b.getDirectory().getAbsolutePath());
			int exit = shellExecutor.executeCommand(env, command);

			if (exit != 0)
				throw new RuntimeException("Did not rebuild bucket correctly: "
						+ b.getDirectory());

			if (logger.isDebugEnabled()) {
				logger.debug(LogFormatter.did("Rebuild bucket", "command was executed",
						null, "stderr", shellExecutor.getStdErr(), "stdout",
						shellExecutor.getStdOut()));
			}
			return b;
		}
	}
}
