public class TestEvent extends Event {

    public TestEvent(String sourceId) {
        super(sourceId, "text/plain", "Ztreamy-java-test");
        setBody(String.valueOf(System.nanoTime()) + "\r\n");
    }
}
