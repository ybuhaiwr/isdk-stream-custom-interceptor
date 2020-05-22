package com.worldremit.stream;

import com.appdynamics.agent.api.AppdynamicsAgent;
import com.appdynamics.agent.api.EntryTypes;
import com.appdynamics.agent.api.ExitCall;
import com.appdynamics.agent.api.Transaction;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.util.List;
import java.util.Optional;

public class StreamWRInterceptor extends WRInterceptor {

    private static final String PROCESS_METHOD = "process";

    @Override
    public Object onMethodBegin(Object invokedObject, String className, String methodName, Object[] paramValues) {
        getLogger().info("CONSUMER-INTERCEPTOR BEGIN: " + className + "#" + methodName);
        startTransactionFromSourceNode(invokedObject);
        return this;
    }

    private void startTransactionFromSourceNode(Object invokedObject) {
        Optional.ofNullable(invokeGetProcessorContext(invokedObject))
                .map(this::invokeGetHeaders)
                .map(this::invokeGetLastHeader)
                .map(this::invokeGetHeaderValue)
                .map(this::bytesToString)
                .ifPresentOrElse(header -> {
                    getLogger().info("Found singularity header " + header);
                    startTransaction(header);
                }, () -> {
                    getLogger().warn("Singularity header not found.");
                });
    }

    private void startTransaction(String stringHeaderValue) {
        Transaction tx = AppdynamicsAgent.startTransaction("CustomInterceptor", stringHeaderValue, EntryTypes.POJO, false);
        ExitCall exitCall = tx.startExitCall("Dummy", "Dummy", "Kafka", false);
        getLogger().info("Exitcall.getCorrelationHeader:" + exitCall.getCorrelationHeader());
        exitCall.end();

        getLogger().info("Started transaction " + tx.getUniqueIdentifier());
    }

    @Override
    public void onMethodEnd(Object state, Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue) {
        getLogger().info("CONSUMER Leaving: " + className + "#" + methodName);
        endTransaction();
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

    /**
     * Build instrumentation rule: tell the agent around which method we would like to instrument.
     *
     * @return
     */
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
        return List.of(buildSourceNodeProcessRule());
    }

}
