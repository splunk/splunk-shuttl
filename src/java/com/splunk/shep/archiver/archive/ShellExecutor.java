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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Executes a command and waits for it to finish.
 */
public class ShellExecutor {

    private static final Logger logger = Logger.getLogger(ShellExecutor.class);

    private final Runtime runtime;

    public ShellExecutor(Runtime runtime) {
	this.runtime = runtime;
    }

    /**
     * @return exit code of the executed command.
     */
    public int executeCommand(String... command) {
	Process process = runCommand(command);
	return waitForProcessToExit(process);
    }

    private Process runCommand(String[] command) {
	try {
	    return runtime.exec(command);
	} catch (IOException e) {
	    logger.error(did("Executed a command with runtime", e,
		    "Command to be executed", "command", command));
	    throw new RuntimeException(e);
	}
    }

    private int waitForProcessToExit(Process exportProcess) {
	try {
	    return exportProcess.waitFor();
	} catch (InterruptedException e) {
	    logger.debug(did("Waited for csv export process to finish.", e,
		    "It to finish."));
	    return -1;
	}
    }

    /**
     * @return
     */
    public static ShellExecutor getInstance() {
	return new ShellExecutor(Runtime.getRuntime());
    }

}
