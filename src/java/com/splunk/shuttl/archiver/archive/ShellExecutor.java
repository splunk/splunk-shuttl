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
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Executes a command and waits for it to finish.
 */
public class ShellExecutor {

    private static final Logger logger = Logger.getLogger(ShellExecutor.class);

    private final Runtime runtime;

    private Process process;

    public ShellExecutor(Runtime runtime) {
	this.runtime = runtime;
    }

    /**
     * @param environment
     *            variables to run with.
     * @return exit code of the executed command.
     */
    public int executeCommand(Map<String, String> env, String... command) {
	process = runCommand(command, env);
	return waitForProcessToExit();
    }

    private Process runCommand(String[] command, Map<String, String> env) {
	try {
	    String[] keyValues = getKeyValuesFromEnv(env);
	    System.out.println(Arrays.toString(keyValues));
	    return runtime.exec(command, keyValues);
	} catch (IOException e) {
	    logger.error(did("Executed a command with runtime", e,
		    "Command to be executed", "command", command));
	    throw new RuntimeException(e);
	}
    }

    private String[] getKeyValuesFromEnv(Map<String, String> env) {
	List<String> keyValues = new ArrayList<String>();
	for (Entry<String, String> keyValue : env.entrySet()) {
	    keyValues.add(keyValue.getKey() + "=" + keyValue.getValue());
	}
	String[] kvs = new String[keyValues.size()];
	for (int i = 0; i < keyValues.size(); i++) {
	    kvs[i] = keyValues.get(i);
	}
	return kvs;
    }

    private int waitForProcessToExit() {
	try {
	    return process.waitFor();
	} catch (InterruptedException e) {
	    logger.debug(did("Waited for csv export process to finish.", e,
		    "It to finish."));
	    return 3;
	}
    }

    /**
     * @return
     */
    public static ShellExecutor getInstance() {
	return new ShellExecutor(Runtime.getRuntime());
    }

    /**
     * @return
     */
    public List<String> getStdOut() {
	try {
	    return IOUtils.readLines(process.getInputStream());
	} catch (IOException e) {
	    return Collections.emptyList();
	}
    }

}
