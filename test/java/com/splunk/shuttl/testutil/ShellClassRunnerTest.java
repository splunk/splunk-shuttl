package com.splunk.shuttl.testutil;

import static org.testng.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "slow-unit" })
public class ShellClassRunnerTest {

    public static final Integer EXIT_CODE = 3;

    private static final Integer EXIT_CODE_FOR_ONE_ARGUMENT = 2;
    private ShellClassRunner shellClassRunner;

    @BeforeMethod(groups = { "slow-unit" })
    public void setUp() {
	shellClassRunner = new ShellClassRunner();
    }

    public void runClassAsync_classWithSystemExit_getTheExitCode() {
	shellClassRunner.runClassAsync(ClassWithMain.class);
	assertEquals(EXIT_CODE, shellClassRunner.getExitCode());
    }

    public static class ClassWithMain {

	public static void main(String[] args) {
	    if (args.length == 1)
		System.exit(EXIT_CODE_FOR_ONE_ARGUMENT);
	    System.exit(EXIT_CODE);
	}
    }

    @Test(groups = { "slow-unit" })
    public void should_returnItSelf_after_runningClassAsync_for_methodChaining() {
	assertEquals(shellClassRunner,
		shellClassRunner.runClassAsync(ClassWithMain.class));
    }

    public void should_returnNull_if_getExitCode_isCalled_before_runClassAsync() {
	assertNull(shellClassRunner.getExitCode());
    }

    public void should_useTheArguments_when_callingClassWithMain() {
	shellClassRunner.runClassAsync(ClassWithMain.class, "argument");
	assertEquals(EXIT_CODE_FOR_ONE_ARGUMENT, shellClassRunner.getExitCode());
    }

    public void getStdOut_afterWaitingToFinish_getOutputFromClass() {
	shellClassRunner.runClassAsync(ClassWithOutput.class);
	shellClassRunner.waitToFinish();
	assertEquals(ClassWithOutput.getOutput(), shellClassRunner.getStdOut());
    }

    public void getStdOut_beforeWaitingToFinish_null() {
	shellClassRunner.runClassAsync(ClassWithOutput.class);
	assertNull(shellClassRunner.getStdOut());
    }

    public void getStdErr_beforeWaitingToFinish_null() {
	shellClassRunner.runClassAsync(ClassWithOutput.class);
	assertNull(shellClassRunner.getStdErr());
    }

    public void runClassAsync_classWithoutArguments_getItsExitCode() {
	shellClassRunner.runClassAsync(ClassWithMain.class);
	assertEquals(EXIT_CODE, shellClassRunner.getExitCode());
    }

    public static class ClassWithOutput {
	public static void main(String[] args) {
	    for (String s : getOutput())
		System.out.println(s);
	}

	public static List<String> getOutput() {
	    List<String> list = new ArrayList<String>();
	    list.add("foo");
	    list.add("bar");
	    return list;
	}
    }

    public void runClassAsync_classWithExternalDependencies_getItsExitCode() {
	shellClassRunner.runClassAsync(ClassWithExternalDependencies.class);
	assertEquals(EXIT_CODE, shellClassRunner.getExitCode());
    }

    public void getJavaExecutablePath_withNoJAVA_HOME_executablePathIsjava() {
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		assertEquals("java", shellClassRunner.getJavaExecutablePath());
	    }
	});
    }

    public void getJavaExecutablePath_withJAVA_HOMEset_executablePathIsFromJavaHome() {
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		String javaHome = "/java/home";
		UtilsEnvironment.setEnvironmentVariable("JAVA_HOME", javaHome);
		assertEquals(javaHome + "/bin/java",
			shellClassRunner.getJavaExecutablePath());
	    }
	});
    }

    public void getOutputStreamToClass_writingExitCodeToItsStdIn_exitWithTheWrittenExitCode()
	    throws IOException {
	shellClassRunner.runClassAsync(ExitWithIntegerWrittenToIt.class);
	OutputStream out = shellClassRunner.getOutputStreamToClass();
	int exitCodeToWrite = 17;
	out.write(exitCodeToWrite);
	out.flush();
	out.close();
	assertEquals(exitCodeToWrite, (int) shellClassRunner.getExitCode());
    }

    private static class ExitWithIntegerWrittenToIt {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException {
	    InputStream in = System.in;
	    while (in.available() == 0)
		;
	    int read = in.read();
	    in.close();
	    System.exit(read);
	}
    }

    @Test(timeOut = 3000)
    public void getInputStreamFromClass_readingExitCodeFromItsStdOut_exitCodeEqualsWrittenByte()
	    throws IOException {
	shellClassRunner.runClassAsync(WriteExitCodeToStdOut.class);
	InputStream in = shellClassRunner.getInputStreamFromClass();
	int exitCodeRead = in.read();
	assertEquals((int) shellClassRunner.getExitCode(), exitCodeRead);
    }

    private static class WriteExitCodeToStdOut {
	@SuppressWarnings("unused")
	public static void main(String[] args) {
	    int exit = 10;
	    System.out.write(exit);
	    System.out.flush();
	    System.exit(exit);
	}
    }

    public void kill_classThatRunsForever_nonZeroExitCode() {
	shellClassRunner.runClassAsync(RunForever.class);
	shellClassRunner.kill();
	assertTrue(shellClassRunner.getExitCode() > 0);
    }

    public void kill_classThatFinishes_exitStatusShouldBeTheSame() {
	shellClassRunner.runClassAsync(ClassWithMain.class);
	Integer exit = shellClassRunner.getExitCode();
	shellClassRunner.kill();
	assertEquals(exit, shellClassRunner.getExitCode());
    }

    public void kill_runBeforeClassStarts_nothing() {
	shellClassRunner.kill();
    }

    private static class RunForever {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws InterruptedException {
	    while (true) {
		Thread.sleep(1000);
	    }
	}
    }
}
