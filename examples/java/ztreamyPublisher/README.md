This directory contains a simple Java library to send test events for
publication through a Ztreamy stream.

An example of how to use the library:

```java
String streamURL = "http://localhost:9000/events/stream
Serializer serializer = new JSONSerializer();
Publisher publisher = new Publisher(new URL(streamURL), serializer);
String sourceId = Event.createUUID();
int result = publisher.publish(new TestEvent(sourceId), true);
if (result == 200) {
    // The server accepted the event
    System.out.println("An event has just been sent to the server");
} else {
    // Something failed
    System.out.println("The server responded with error " + result);
}
```

In this example, we use a custom event class called TestEvent
that extends the base Event class the library provides:

```java
public class TestEvent extends Event {

    public TestEvent(String sourceId) {
        super(sourceId, "application/json", "Ztreamy-java-test", "Test event",
              new LinkedHashMap<String, Object>());
        getBody().put("timestamp", String.valueOf(System.nanoTime()));
    }
}
```

The full code of an example is available at the directory src/test/java/

The library requires the Google Gson library for JSON support:

https://code.google.com/p/google-gson/

You need to download its JAR file and place it in your CLASSPATH
in order to run this example.

If you want to use this code from Android, you'll probably want to
write an alternative JSONSerializer class that uses the JSON
support already provided by Android. If you do that, I would appreciate
it if you contributed your implementation back, so that I can
make it available with the rest of this code.
