package com.worldremit.stream;

import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.util.function.Function;

public abstract class WRInterceptor extends AGenericInterceptor {

    protected final IReflector removeHeaderInvocation;
    protected final IReflector setHeaderInvocation;
    protected final IReflector getHeaderInvocation;
    protected final IReflector valueReflector;
    protected final IReflector recordContextReflector;
    protected final IReflector headersReflector;
    protected final IReflector lastHeaderReflector;
    protected final IReflector accessProcessorContextReflector;

    public WRInterceptor() {
        removeHeaderInvocation = getNewReflectionBuilder()
                .invokeInstanceMethod("remove", true, "java.lang.String")
                .build();
        setHeaderInvocation = getNewReflectionBuilder()
                .invokeInstanceMethod("add", true, "java.lang.String", "[B")
                .build();
        getHeaderInvocation = getNewReflectionBuilder()
                .invokeInstanceMethod("lastHeader", true, "java.lang.String")
                .build();
        valueReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("value", true).build();
        recordContextReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("recordContext", true)
                .build();
        headersReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("headers", true)
                .build();
        lastHeaderReflector = getNewReflectionBuilder()
                .invokeInstanceMethod("lastHeader", true, "java.lang.String")
                .build();
        accessProcessorContextReflector = getNewReflectionBuilder().accessFieldValue("context", true)
                .build();
    }

    public String bytesToString(Object bytes) {
        if (bytes instanceof byte[]) {
            return new String((byte[]) bytes);
        } else {
            return null;
        }
    }

    protected Object invokeGetValue(Object currentSingularityHeader) throws ReflectorException {
        return valueReflector.execute(this.getClass().getClassLoader(), currentSingularityHeader, (Object[]) null);
    }

    protected Object invokeGetSingularityHeader(Object headers) throws ReflectorException {
        return getHeaderInvocation.execute(this.getClass().getClassLoader(), headers, new Object[]{"singularityheader"});
    }

    protected Object invokeGetHeaderValue(Object singularityHeader) {
        try {
            return valueReflector
                    .execute(classLoader(), singularityHeader, (Object[]) null);
        } catch (ReflectorException e) {
            getLogger().error("Reflection failed", e);
            return null;
        }
    }

    protected Object invokeGetLastHeader(Object headers) {
        try {
            return lastHeaderReflector
                    .execute(classLoader(), headers, new Object[]{"singularityheader"});
        } catch (ReflectorException e) {
            getLogger().error("Reflection failed", e);
            return null;
        }
    }

    protected Object invokeGetHeaders(Object processorContext) {
        try {
            return headersReflector.execute(classLoader(), processorContext, (Object[]) null);
        } catch (ReflectorException e) {
            getLogger().warn("Reflection error, unable to get Headers", e);
            return null;
        }
    }

    protected Object invokeGetProcessorContext(Object invokedObject) {
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

    private void printStackTrace() {
        getLogger().debug("Stack trace:");
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            getLogger().debug(" " + stackTraceElement.toString());
        }
    }

    @FunctionalInterface
    public interface UnsafeReflectorFunction<T,R> {
        R apply(T t) throws ReflectorException;
    }

    /**
     * Converts the lambda expression throwing ReflectorException so it returns null.
     *
     * @param function
     * @param <T>
     * @param <R>
     * @return
     */
    public <T,R> Function<T,R> safeReflector(UnsafeReflectorFunction<T,R> function) {
        return t -> {
            try {
                return function.apply(t);
            } catch (ReflectorException e) {
                getLogger().error("Reflection error: ", e);
                return null;
            }
        };
    }


}
