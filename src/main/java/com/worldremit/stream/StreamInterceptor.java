package com.worldremit.stream;

import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.*;

public class StreamInterceptor extends AGenericInterceptor {


    @Override
    public Object onMethodBegin(Object invokedObject, String className, String methodName, Object[] paramValues) {

        return null;
    }

    @Override
    public void onMethodEnd(Object state, Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue) {

    }

    @Override
    public List<Rule> initializeRules() {
        Rule build = new Rule.Builder("org.apache.kafka.streams.processor.internals.StreamTask")
                .classMatchType(SDKClassMatchType.MATCHES_CLASS)
                .atMethodInvocation("process", 1)
                .atLocalVariable("record", false, 1)
                .build();

        return singletonList(build);
    }
}
