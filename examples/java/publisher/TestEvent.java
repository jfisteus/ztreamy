import java.util.LinkedHashMap;
import java.util.Map;

public class TestEvent extends Event {

    public TestEvent(String sourceId) {
        super(sourceId, "application/json", "Ztreamy-java-test");
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("timestamp", String.valueOf(System.nanoTime()));
        setBody(body);
    }
}
