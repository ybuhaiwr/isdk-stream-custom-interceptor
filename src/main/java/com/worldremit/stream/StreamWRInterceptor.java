package com.worldremit.stream;

import com.appdynamics.agent.api.AppdynamicsAgent;
import com.appdynamics.agent.api.EntryTypes;
import com.appdynamics.agent.api.Transaction;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflectionBuilder;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.*;

public class StreamWRInterceptor extends AGenericInterceptor {

    private final IReflector recordContextReflector;
    private final IReflector headersReflector;
    private final IReflector lastHeaderReflector;
    private final IReflector valueReflector;
    private IReflector accessProcessorContextReflector;

    public StreamWRInterceptor() {
        accessProcessorContextReflector = getNewReflectionBuilder().accessFieldValue("processorContext", true)
                .build();
        recordContextReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("recordContext", true)
                .build();
        headersReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("headers", true)
                .build();
        lastHeaderReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("lastHeader", true, "java.lang.String")
                .build();
        valueReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("value", true).build();
    }

    @Override
    public Object onMethodBegin(Object invokedObject, String className, String methodName, Object[] paramValues) {
        getLogger().info("INTERCEPTOR BEGIN: " + className + "#" + methodName);

        getLogger().info("Stack trace:");
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            getLogger().info(" " + stackTraceElement.toString());
        }


        try {

            Object processorContext = accessProcessorContextReflector
                    .execute(this.getClass().getClassLoader(), invokedObject, (Object[]) null);

            getLogger().info("INTERCEPTOR Processor context is " + processorContext.getClass().getName() + " " + processorContext.toString());

            if (processorContext == null) {
                getLogger().warn("ProcessorContext is null");
                return null;
            }

            getLogger().info("ProcessorContext is " + Integer.toHexString(System.identityHashCode(processorContext)));

            Object recordContext = recordContextReflector
                    .execute(this.getClass().getClassLoader(), processorContext, (Object[]) null);

            if (recordContext == null) {
                getLogger().warn("recordContext is null");
                return null;
            }



            Object headers = headersReflector.execute(this.getClass().getClassLoader(), recordContext, (Object[]) null);

            if (headers == null) {
                getLogger().warn("headers is null");
                return null;
            } else {
                getLogger().info("recordContext.headers " + headers.toString());
            }

            Object singularityHeader = lastHeaderReflector
                    .execute(this.getClass().getClassLoader(), headers, new Object[]{"singularityheader"});

            if (singularityHeader == null) {
                getLogger().warn("Singularity header value is null");
                return null;
            } else {
                getLogger().info("Singularity header found: " + singularityHeader);
            }

            Object headerValue = valueReflector
                    .execute(this.getClass().getClassLoader(), singularityHeader, (Object[]) null);

            if (headerValue instanceof byte[]) {
                String stringHeaderValue = new String((byte[]) headerValue);
                getLogger().info("INTERCEPTOR singularity Header String" + stringHeaderValue);

                Transaction tx = AppdynamicsAgent.startTransaction("CustomInterceptor", stringHeaderValue, EntryTypes.POJO, true);
                getLogger().info("Started transaction " + tx.getUniqueIdentifier());
            } else {
                getLogger().warn("Singularity header is not a byte[]??? " + headerValue);
            }

            getLogger().info("INTERCEPTOR singularity Header " + headerValue);

        } catch (ReflectorException e) {
            getLogger().error("INTERCEPTOR Reflection failed", e);
        }

        return null;
    }

    @Override
    public void onMethodEnd(Object state, Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue) {
        getLogger().info("INTERCEPTOR END: " + className + "#" + methodName);

        Transaction transaction = AppdynamicsAgent.getTransaction();
        if (transaction != null) {
            getLogger().info("Ending transaction " + transaction.getUniqueIdentifier());
            transaction.end();
        } else {
            getLogger().warn("Null transaction on intercepted method end.");
        }
    }


    @Override
    public List<Rule> initializeRules() {
        Rule build = new Rule.Builder("org.apache.kafka.streams.processor.internals.StreamTask")
                .classMatchType(SDKClassMatchType.MATCHES_CLASS)
                .classStringMatchType(SDKStringMatchType.EQUALS)
                .methodMatchString("process")
                .methodStringMatchType(SDKStringMatchType.EQUALS)
//                .atField("processorContext", true, 2)

//                .isAbsoluteLineNumber(true)
//                .atLineNumber(363)

                .atMethodInvocation("updateProcessorContext", 1)
                .isInstrumentBefore(false)

//                .atMethodInvocation("process", 1)
//                .atLocalVariable("record", false, 1)

//                .atLocalVariable("currNode", true, 1)

                .build();

        return singletonList(build);
    }
}
