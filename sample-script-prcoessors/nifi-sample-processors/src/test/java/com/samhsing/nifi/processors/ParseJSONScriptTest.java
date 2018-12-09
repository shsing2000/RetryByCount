package com.samhsing.nifi.processors;

import java.nio.charset.StandardCharsets;

import org.apache.nifi.processors.script.ExecuteScript;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class ParseJSONScriptTest {

    private TestRunner testRunner;
    private ScriptingComponentHelper scHelper;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(new ExecuteScript());
        testRunner.setValidateExpressionUsage(false);
        scHelper = new ScriptingComponentHelper();
        scHelper.createResources();
    }

    @Test
    public void testProcessor() {
        testRunner.setProperty(scHelper.SCRIPT_ENGINE, "ECMAScript");
        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/classes/scripts/ParseJSONScript.js");
        testRunner.assertValid();
        testRunner.enqueue("{}".getBytes(StandardCharsets.UTF_8));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
    }
}
