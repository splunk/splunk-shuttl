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

package com.splunk.shuttl.testutil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.splunk.shuttl.archiver.archive.BucketArchiver;

/**
 * Run a class in a new JVM and invoke the class' main method with arguments.
 */
public class ShellClassRunner {

    private Integer exitCode = null;
    private List<String> stdOut;
    private List<String> stdErr;
    private Process runClassProcess;

    /**
     * Starts a process running the class parameter with given arguments. The
     * process is run async until {@link ShellClassRunner#waitToFinish()} or
     * {@link ShellClassRunner#getExitCode()} is called.
     */
    public ShellClassRunner runClassAsync(Class<?> clazz, String... args) {
	String execString = getExecutableStringForClassAndArgs(clazz, args);
	runClassProcess = doRunArchiveBucket(execString);
	return this;
    }

    private String getExecutableStringForClassAndArgs(Class<?> clazz,
	    String... args) {
	String fullClassName = clazz.getName();
	String execString = getJavaExecutablePath() + " -cp \":"
		+ getClasspath() + ":\" " + fullClassName + " "
		+ getSpaceSeparatedArgs(args);
	return execString;
    }

    private Process doRunArchiveBucket(String execString) {
	try {
	    return Runtime.getRuntime().exec(execString);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    /**
     * Choose a class that's in src and one that's in test, to get their class
     * paths.
     */
    private String getClasspath() {
	String srcClasspath = BucketArchiver.class.getProtectionDomain()
		.getCodeSource().getLocation().getPath();
	String testClasspath = ShellClassRunnerTest.class.getProtectionDomain()
		.getCodeSource().getLocation().getPath();
	String libClasspath = "lib/*";
	return srcClasspath + ":" + testClasspath + ":" + libClasspath;
    }

    private String getSpaceSeparatedArgs(String[] args) {
	StringBuilder sb = new StringBuilder();
	for (String s : args)
	    sb.append(s + " ");
	return sb.toString();
    }

    /**
     * Waits for the class run finish running.
     */
    public void waitToFinish() {
	waitForExitCodeAndSetInAndOutputStreams();
    }

    private void waitForExitCodeAndSetInAndOutputStreams() {
	exitCode = getExitCodeFromProcess();
	stdOut = readInputStream(runClassProcess.getInputStream());
	stdErr = readInputStream(runClassProcess.getErrorStream());
    }

    private int getExitCodeFromProcess() {
	try {
	    return runClassProcess.waitFor();
	} catch (Exception e) {
	    printStdOutAndErr();
	    throw new RuntimeException(e);
	}
    }

    private void printStdOutAndErr() {
	try {
	    System.err.println(IOUtils.readLines(runClassProcess
		    .getErrorStream()));
	    System.err.println(IOUtils.readLines(runClassProcess
		    .getInputStream()));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private List<String> readInputStream(InputStream inputStream) {
	try {
	    return IOUtils.readLines(inputStream);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    public Integer getExitCode() {
	if (!hasStartedProcess()) {
	    return null;
	}
	if (exitCode == null) {
	    waitToFinish();
	}
	return exitCode;
    }

    private boolean hasStartedProcess() {
	return runClassProcess != null;
    }

    public List<String> getStdOut() {
	return stdOut;
    }

    public List<String> getStdErr() {
	return stdErr;
    }

    public OutputStream getOutputStreamToClass() {
	return runClassProcess.getOutputStream();
    }

    public InputStream getInputStreamFromClass() {
	return runClassProcess.getInputStream();
    }

    /**
     * @return path to java, the executable.
     */
    /* package-private */String getJavaExecutablePath() {
	Map<String, String> env = System.getenv();
	if (env.containsKey("JAVA_HOME")) {
	    return env.get("JAVA_HOME") + "/bin/java";
	} else {
	    return "java";
	}
    }

    /**
     * Kills the running class.
     */
    public void kill() {
	if (hasStartedProcess()) {
	    runClassProcess.destroy();
	    exitCode = getExitCodeFromProcess();
	}
    }

}
