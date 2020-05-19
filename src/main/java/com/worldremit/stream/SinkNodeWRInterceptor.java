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

import static java.util.Collections.singletonList;

public class SinkNodeWRInterceptor extends AGenericInterceptor {

    private final IReflector removeHeaderInvocation;
    private final IReflector setHeaderInvocation;
    private final IReflector getHeaderInvocation;

    public SinkNodeWRInterceptor() {
        removeHeaderInvocation = getNewReflectionBuilder()
                .invokeInstanceMethod("remove", true, "java.lang.String")
                .build();

        setHeaderInvocation = getNewReflectionBuilder()
                .invokeInstanceMethod("add", true, "java.lang.String", "byte[]")
                .build();

        getHeaderInvocation = getNewReflectionBuilder()
                .invokeInstanceMethod("lastHeader", true, "java.lang.String")
                .build();

    }

    @Override
    public Object onMethodBegin(Object invokedObject, String className, String methodName, Object[] paramValues) {
        getLogger().info("Entering send()");
        String topicName = (String) paramValues[0];
        Object headers = paramValues[3];
        if (headers == null) {
            getLogger().warn("Headers argument is null for "+className+"#"+methodName);
            return null;
        }

        Transaction transaction = AppdynamicsAgent.getTransaction();
        if (transaction == null) {
            getLogger().warn("Transaction is null.");
            return null;
        }
        ExitCall exitCall = transaction.startExitCall(topicName, topicName, "Kafka", false);
        if (exitCall == null) {
            getLogger().warn("ExitCall is null.");
            return null;
        }
        String correlationHeader = exitCall.getCorrelationHeader();

        getLogger().info("Exit call singularity header: " + correlationHeader);

        try {
            Object currentSingularityHeader = getHeaderInvocation.execute(this.getClass().getClassLoader(), headers, new Object[]{"singularityheader"});
            getLogger().info("Current singularity header: " + currentSingularityHeader);
            removeHeaderInvocation.execute(this.getClass().getClassLoader(), headers, new Object[]{"singularityheader"});
            setHeaderInvocation.execute(this.getClass().getClassLoader(), headers, new Object[]{"singularityheader", correlationHeader.getBytes()});
        } catch (ReflectorException e) {
            getLogger().error("Reflection error", e);
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void onMethodEnd(Object state, Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue) {

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
