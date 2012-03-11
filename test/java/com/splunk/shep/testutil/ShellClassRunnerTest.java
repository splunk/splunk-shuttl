package com.splunk.shep.testutil;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "slow" })
public class ShellClassRunnerTest {

    public static final Integer EXIT_CODE = 1;

    private static final Integer EXIT_CODE_FOR_ONE_ARGUMENT = 2;
    private ShellClassRunner shellClassRunner;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	shellClassRunner = new ShellClassRunner();
    }

    public void should_executeClassWithSystemExit_throughShell_without_killingThisJavaProcess() {
	shellClassRunner.runClassWithArgs(ClassWithMain.class);
	assertEquals(EXIT_CODE, shellClassRunner.getExitCode());
    }

    public static class ClassWithMain {

	public static void main(String[] args) {
	    if (args.length == 1)
		System.exit(EXIT_CODE_FOR_ONE_ARGUMENT);
	    System.exit(EXIT_CODE);
	}
    }

    public void should_returnItSelf_after_runningClassWithArgs_for_methodChaining() {
	assertEquals(shellClassRunner,
		shellClassRunner.runClassWithArgs(ClassWithMain.class));
    }

    public void should_returnNull_if_getExitCode_isCalled_before_runClassWithArgs() {
	assertNull(shellClassRunner.getExitCode());
    }

    public void should_useTheArguments_when_callingClassWithMain() {
	shellClassRunner.runClassWithArgs(ClassWithMain.class, "argument");
	assertEquals(EXIT_CODE_FOR_ONE_ARGUMENT, shellClassRunner.getExitCode());
    }

    public void should_beAbleToGetStdOut_after_runningClassWithOutput() {
	shellClassRunner.runClassWithArgs(ClassWithOutput.class);
	assertEquals(ClassWithOutput.getOutput(), shellClassRunner.getStdOut());
    }

    public void should_beAbleToRunClass_withoutArguments() {
	shellClassRunner.runClassWithArgs(ClassWithMain.class);
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

    public void should_beAbleToRunClass_withExternalDependencies() {
	shellClassRunner.runClassWithArgs(ClassWithExternalDependencies.class);
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
}
