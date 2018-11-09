/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.samhsing.nifi.processors;

import org.apache.nifi.processors.script.ExecuteScript;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class BasicScriptTest {

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
        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/classes/scripts/BasicScript.js");
        testRunner.assertValid();
        testRunner.enqueue(new byte[0]);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
    }
}
