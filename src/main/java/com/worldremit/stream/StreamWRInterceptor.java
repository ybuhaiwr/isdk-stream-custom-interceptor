package com.worldremit.stream;

import com.appdynamics.agent.api.AppdynamicsAgent;
import com.appdynamics.agent.api.EntryTypes;
import com.appdynamics.agent.api.Transaction;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.util.List;
import java.util.Optional;

public class StreamWRInterceptor extends AGenericInterceptor {

    private static final String UPDATE_PROCESSOR_CONTEXT_METHOD = "updateProcessorContext";
    private static final String PROCESS_METHOD = "process";

    private final IReflector recordContextReflector;
    private final IReflector headersReflector;
    private final IReflector lastHeaderReflector;
    private final IReflector valueReflector;
    private IReflector accessProcessorContextReflector;

    public StreamWRInterceptor() {
        accessProcessorContextReflector = getNewReflectionBuilder().accessFieldValue("context", true)
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
        getLogger().info("CONSUMER-INTERCEPTOR BEGIN: " + className + "#" + methodName);

        if (methodName.equals(UPDATE_PROCESSOR_CONTEXT_METHOD)) {
            Optional.ofNullable(getHeaders(paramValues[0]))
                    .map(o -> getLastHeader(o))
                    .map(o -> getHeaderValue(o))
                    .map(o -> bytesToString(o))
                    .ifPresentOrElse(header -> {
                        getLogger().info("Found singularity header " + header);
                        startTransaction(header);
                    }, () -> {
                        getLogger().warn("Singularity header not found.");
                    });
        }


        return null;
    }

    private void startTransactionFromSourceNode(Object invokedObject) {
        Object processorContext = getProcessorContext(invokedObject);
        // ----
        Optional.ofNullable(processorContext)
                .map(pc -> getHeaders(pc))
                .map(o -> getLastHeader(o))
                .map(o -> getHeaderValue(o))
                .map(o -> bytesToString(o))
                .ifPresentOrElse(header -> {
                    getLogger().info("Found singularity header " + header);
                    startTransaction(header);
                }, () -> {
                    getLogger().warn("Singularity header not found.");
                });
    }

    private void printStackTrace() {
        getLogger().debug("Stack trace:");
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            getLogger().debug(" " + stackTraceElement.toString());
        }
    }

    private void startTransaction(String stringHeaderValue) {
        Transaction tx = AppdynamicsAgent.startTransaction("CustomInterceptor", stringHeaderValue, EntryTypes.POJO, true);
        getLogger().info("Started transaction " + tx.getUniqueIdentifier());
    }

    private String bytesToString(Object bytes) {
        if (bytes instanceof byte[]) {
            return new String((byte[]) bytes);
        } else {
            return null;
        }
    }

    private Object getHeaderValue(Object singularityHeader) {
        try {
            return valueReflector
                    .execute(classLoader(), singularityHeader, (Object[]) null);
        } catch (ReflectorException e) {
            getLogger().error("Reflection failed", e);
            return null;
        }
    }

    private Object getLastHeader(Object headers) {
        try {
            return lastHeaderReflector
                    .execute(classLoader(), headers, new Object[]{"singularityheader"});
        } catch (ReflectorException e) {
            getLogger().error("Reflection failed", e);
            return null;
        }
    }

    private Object getHeaders(Object processorContext) {
        try {
            return headersReflector.execute(classLoader(), processorContext, (Object[]) null);
        } catch (ReflectorException e) {
            getLogger().warn("Reflection error, unable to get Headers", e);
            return null;
        }
    }

    private Object getProcessorContext(Object invokedObject) {
        try {
            return accessProcessorContextReflector.execute(classLoader(), invokedObject, (Object[]) null);
        } catch (ReflectorException e) {
            getLogger().warn("Reflection error, unable to get ProcessorContext", e);
            return null;
        }
    }

    private ClassLoader classLoader() {
        return this.getClass().getClassLoader();
    }

    @Override
    public void onMethodEnd(Object state, Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue) {
        getLogger().info("CONSUMER-INTERCEPTOR END: " + className + "#" + methodName);

        if (methodName.equals(PROCESS_METHOD)) {
            endTransaction();
        }

    }

    private void endTransaction() {
        Transaction transaction = AppdynamicsAgent.getTransaction();
        if (transaction != null) {
            getLogger().info("Ending transaction " + transaction.getUniqueIdentifier());
            transaction.end();
        } else {
            getLogger().warn("Null transaction on intercepted method end.");
        }
    }

    private Rule buildStreamTaskProcessRule() {
        return new Rule.Builder("org.apache.kafka.streams.processor.internals.StreamTask")
                .classMatchType(SDKClassMatchType.MATCHES_CLASS)
                .classStringMatchType(SDKStringMatchType.EQUALS)
                .methodMatchString("process")
                .methodStringMatchType(SDKStringMatchType.EQUALS)
                .build();
    }

    private Rule buildStreamTaskUpdateProcessorContextRule() {
        return new Rule.Builder("org.apache.kafka.streams.processor.internals.StreamTask")
                .classMatchType(SDKClassMatchType.MATCHES_CLASS)
                .classStringMatchType(SDKStringMatchType.EQUALS)
                .methodMatchString(UPDATE_PROCESSOR_CONTEXT_METHOD)
                .withParams("org.apache.kafka.streams.processor.internals.StampedRecord",
                            "org.apache.kafka.streams.processor.internals.ProcessorNode")
                .methodStringMatchType(SDKStringMatchType.EQUALS)
                .build();
    }

    private Rule buildSourceNodeProcessRule() {
        return new Rule.Builder("org.apache.kafka.streams.processor.internals.SourceNode")
                .classMatchType(SDKClassMatchType.MATCHES_CLASS)
                .classStringMatchType(SDKStringMatchType.EQUALS)
                .methodMatchString(PROCESS_METHOD)
                .methodStringMatchType(SDKStringMatchType.EQUALS)
                .withParams("java.lang.Object",
                            "java.lang.Object")
                .build();
    }

    @Override
    public List<Rule> initializeRules() {
        return List.of(buildStreamTaskProcessRule(), buildStreamTaskUpdateProcessorContextRule());
    }

}
