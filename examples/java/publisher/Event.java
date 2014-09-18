import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.text.SimpleDateFormat;

public class Event {
    private String eventId;
    private String sourceId;
    private String syntax;
    private String timestamp;
    private String applicationId;
    private byte[] body;
    private Map<String, String> extraHeaders;

    private Charset charsetUTF8 = Charset.forName("UTF-8");
    private static SimpleDateFormat rfc3339Format =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    public Event(String eventId, String sourceId, String syntax,
                 String applicationId, byte[] body) {
        this.eventId = eventId;
        this.sourceId = sourceId;
        this.syntax = syntax;
        this.applicationId = applicationId;
        this.body = body;
        this.extraHeaders = new HashMap<String, String>();
        timestamp = createTimestamp();
    }

    public Event(String eventId, String sourceId, String syntax,
                 String applicationId) {
        this(eventId, sourceId, syntax, applicationId, new byte[0]);
    }

    public Event(String sourceId, String syntax, String applicationId) {
        this(createUUID(), sourceId, syntax, applicationId);
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public void setBody(String bodyAsString) {
        body = bodyAsString.getBytes(charsetUTF8);
    }

    public void setExtraHeader(String name, String value) {
        extraHeaders.put(name, value);
    }

    public byte[] serialize() {
        StringBuffer buffer = new StringBuffer();
        serializeHeader(buffer, "Event-Id", eventId);
        serializeHeader(buffer, "Source-Id", sourceId);
        serializeHeader(buffer, "Syntax", syntax);
        if (applicationId != null) {
            serializeHeader(buffer, "Application-Id", applicationId);
        }
        if (timestamp != null) {
            serializeHeader(buffer, "Timestamp", timestamp);
        }
        for (Map.Entry<String, String> entry: extraHeaders.entrySet()) {
            serializeHeader(buffer, entry.getKey(), entry.getValue());
        }
        serializeHeader(buffer, "Body-Length", String.valueOf(body.length));
        buffer.append("\r\n");
        byte[] headers = buffer.toString().getBytes(charsetUTF8);
        return concatenate(headers, body);
    }

    public static String createUUID() {
        return UUID.randomUUID().toString();
    }

    private static void serializeHeader(StringBuffer buffer, String name,
                                        String value) {
        buffer.append(name);
        buffer.append(": ");
        buffer.append(value);
        buffer.append("\r\n");
    }

    private static String createTimestamp() {
        return rfc3339Format.format(new Date());
    }

    private static byte[] concatenate(byte[] first, byte[] second) {
        byte[] dest = new byte[first.length + second.length];
        System.arraycopy(first, 0, dest, 0, first.length);
        System.arraycopy(second, 0, dest, first.length, second.length);
        return dest;
    }

    public static void main(String[] args) throws IOException {
        Event event = new Event(createUUID(), "text/plain", "Ztreamy-test");
        event.setBody("Test body.");
        System.out.write(event.serialize());
    }
}
