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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"retry"})
@CapabilityDescription("Check a FlowFile attribute to determine whether to route to Retry or Failure relationships. " +
    "The attribute value is compared to a max limit. When the attribute value is less than the limit, the attribute " +
    "is incremented, and the FlowFile is routed to Retry. Otherwise, the FlowFile is routed to Failure.")
@ReadsAttributes({
    @ReadsAttribute(attribute="Counter Attribute", description="FlowFile attribute name to store retry counter"),
    @ReadsAttribute(attribute="Counter Limit", description="Max number of retries before routing to Failure")})
@WritesAttributes({@WritesAttribute(attribute="Counter Attribute", description="FlowFile attribute to store retry counter")})
public class RetryByCountProcessor extends AbstractProcessor {

    public static final PropertyDescriptor COUNTER_ATTR_NAME = new PropertyDescriptor
        .Builder()
        .name("COUNTER_ATTR_NAME")
        .displayName("Counter Attribute")
        .description("Attribute to check/store the current retry count")
        .required(true)
        .defaultValue("retry.counter")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    public static final PropertyDescriptor COUNTER_MAX_LIMIT = new PropertyDescriptor
        .Builder()
        .name("COUNTER_MAX_LIMIT")
        .displayName("Counter Limit")
        .description("Maximum counter value before transferring to the Failure relationship")
        .required(true)
        .defaultValue("3")
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    public static final Relationship RETRY = new Relationship.Builder()
        .name("Retry")
        .description("Retry the request")
        .build();

    public static final Relationship FAILURE = new Relationship.Builder()
        .name("Failure")
        .description("Retry limit reached")
        .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(COUNTER_ATTR_NAME);
        descriptors.add(COUNTER_MAX_LIMIT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(RETRY);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    private int parseWithDefault(String s, int defaultValue) {
        return s.matches("\\d+") ? Integer.parseInt(s) : defaultValue;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {
            final String attrName = context.getProperty(COUNTER_ATTR_NAME).getValue();
            final int maxLimit = context.getProperty(COUNTER_MAX_LIMIT).asInteger();
            int currentCount = flowFile.getAttribute(attrName) == null ? 0 : parseWithDefault(flowFile.getAttribute(attrName), 0);

            if (currentCount < maxLimit) {
                session.putAttribute(flowFile, attrName, Integer.toString(currentCount + 1));
                session.transfer(flowFile, RETRY);
            } else {
                session.transfer(flowFile, FAILURE);
            }
        } catch (final Exception ex) {
            getLogger().error("Failed to process retry count for {}, routing to failure", new Object[]{flowFile, ex});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, FAILURE);
        }
    }
}
