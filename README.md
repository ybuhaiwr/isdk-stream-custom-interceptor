# AppDynamincs Agent plugin for Kafka Streams

This project is a extension to AppDynamics agent which allows correlation of Kafka Streams applications.
The output artifact is single JAR file, which is placed into `<agent-directory>/ver<agent-version>/sdk-plugins`.

The JAR file is automatically detected and loaded by agent. You may inspect the **agent.log** 
file and look for signs of successful (or not) loading. Log files are in  `<agent-directory>/ver<agent-version>/logs`, 
but you need to use Controller UI to access the logs.

For more documentation on the SDK see [AppDynamics SDK documentation](https://docs.appdynamics.com/display/PRO45/Overview+of+the+iSDK).
and the [SDK User Guide](https://docs.appdynamics.com/display/PRO45/Java+Agent+API+User+Guide)
## Instrumentation strategy

Basicly, the trick is easy: you have to spot a method which wraps the processing of Business Transaction,
and other spot where records are published to Kafka.

```java
void processRecord(ConsumerRecord record) { 
   // ... do something

   Record output = getResultOfSomeProcessing();

   sendToKafka(output);
}
```

Using SDK, you need to build a little plumbing around. Invocation to `startTransaction()` 
is actually continuing the existing transaction if provided with singularityheader.

```java
String header = getSingularityHeaderFrom(input);
AppdynamicsAgent.startTransaction("SampleGenericInterceptorBT", header, EntryTypes.POJO, false);

// ... logic

// When it comes to send to kafka, inject the header:
ExitCall exitCall = transaction.startExitCall(topicName, topicName, "Kafka", false);
String correlationHeader = exitCall.getCorrelationHeader();
outputRecord.headers().add("singularityheader", correlationHeader);
exitCall.end();

// Somewhere, later in the code. You don't need to pass transaction object if 
// everything happens on same thread.
Transaction currentTransaction = AppdynamicsAgent.getTransaction();
currentTransaction.end();
```