package com.worldremit.stream;

import com.appdynamics.agent.api.AppdynamicsAgent;
import com.appdynamics.agent.api.ExitCall;
import com.appdynamics.agent.api.ExitTypes;
import com.appdynamics.agent.api.Transaction;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflectionBuilder;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Collections.singletonList;

public class SinkNodeWRInterceptor extends WRInterceptor {

    @Override
    public Object onMethodBegin(Object invokedObject, String className, String methodName, Object[] paramValues) {
        String topicName = (String) paramValues[0];
        Object headers = paramValues[3];

        getLogger().info("Entering " + methodName + "() on topic: " + topicName);

        if (headers == null) {
            getLogger().warn("ABORT: Headers argument is null for "+className+"#"+methodName);
            return null;
        }

        Transaction transaction = AppdynamicsAgent.getTransaction();
        if (transaction == null) {
            getLogger().warn("ABORT: Current transaction is null.");
            return null;
        } else {
            getLogger().info("Found transaction: " + transaction.getUniqueIdentifier());
        }

        ExitCall exitCall = transaction.startExitCall(topicName, topicName, "Kafka", false);
        if (exitCall == null) {
            getLogger().warn("ABORT: ExitCall is null.");
            return null;
        }
        String correlationHeader = exitCall.getCorrelationHeader();
        getLogger().info("Singularity header to be set: " + correlationHeader);
        if (correlationHeader != null) {
            try {
                Optional.ofNullable(invokeGetSingularityHeader(headers))
                        .map(safeReflector(this::invokeGetValue))
                        .map(this::bytesToString)
                        .ifPresentOrElse(s -> getLogger().info("Curent singularity header: " + s),
                                         () -> getLogger().warn("Singularity header not found"));

                removeHeaderInvocation.execute(this.getClass().getClassLoader(), headers, new Object[]{"singularityheader"});
                getLogger().info("Setting new singularity header: " + correlationHeader);
                setHeaderInvocation.execute(this.getClass().getClassLoader(), headers, new Object[]{"singularityheader", correlationHeader.getBytes()});
            } catch (ReflectorException e) {
                getLogger().error("ABORT: Reflection error", e);
                e.printStackTrace();
            }
        } else {
            getLogger().warn("Not setting exit singularity header, because got null from ExitCall.");
        }

        return exitCall;
    }

    @Override
    public void onMethodEnd(Object state, Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue) {
        getLogger().info("Leaving " + methodName + "()");
        if (state != null) {
            getLogger().info("Exit call is: " + state.getClass().getName());
            ExitCall exitCall = (ExitCall) state;
            getLogger().info("Ending ExitCall, correlation header: " + exitCall.getCorrelationHeader());
            exitCall.end();
        } else {
            getLogger().warn("ExitCall not received from onMethodBegin()");
        }
    }


    @Override
    public List<Rule> initializeRules() {
        Rule build = new Rule.Builder("org.apache.kafka.streams.processor.internals.RecordCollectorImpl")
                .classMatchType(SDKClassMatchType.MATCHES_CLASS)
                .classStringMatchType(SDKStringMatchType.EQUALS)
                .methodMatchString("send")
                .withParams("java.lang.String",
                            "java.lang.Object",
                            "java.lang.Object",
                            "org.apache.kafka.common.header.Headers",
                            "java.lang.Integer",
                            "java.lang.Long",
                            "org.apache.kafka.common.serialization.Serializer",
                            "org.apache.kafka.common.serialization.Serializer")
                .methodStringMatchType(SDKStringMatchType.EQUALS)
                .build();

        return singletonList(build);
    }
}
