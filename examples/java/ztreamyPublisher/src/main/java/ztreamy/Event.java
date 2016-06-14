package ztreamy;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.text.SimpleDateFormat;
import com.google.gson.Gson;

/**
 * A Ztreamy event with all its fields.
 *
 */
public class Event {
    private String eventId;
    private String sourceId;
    private String syntax;
    private String timestamp;
    private String applicationId;
    private String eventType;
    private Map<String, Object> body;
    private Map<String, String> extraHeaders;

    private static Charset charsetUTF8 = Charset.forName("UTF-8");
    private static SimpleDateFormat rfc3339Format =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    /**
     * Create an event with all the required parameters.
     *
     * @param eventId the id of the new event (normally a UUID).
     * @param sourceId the id of the source of this event (normally a UUID).
     * @param syntax the syntax of the event body (a MIME type).
     * @param applicationId the identifier of the application to which
     *            the event belongs.
     * @param eventType the application-specific type of event.
     * @param body the body of the event as a map. The map can contain nested
     *            maps for defining complex structures. When the event
     *            is not JSON, the map must contain a single key "value"
     *            with a String value.
     *
     */
    public Event(String eventId, String sourceId, String syntax,
                 String applicationId, String eventType,
                 Map<String, Object> body) {
        this.eventId = eventId;
        this.sourceId = sourceId;
        this.syntax = syntax;
        this.applicationId = applicationId;
        this.eventType = eventType;
        this.body = body;
        this.extraHeaders = new LinkedHashMap<String, String>();
        timestamp = createTimestamp();
    }

    /**
     * Create an event with an auto-generated event id.
     *
     * @param sourceId the id of the source of this event (normally a UUID).
     * @param syntax the syntax of the event body (a MIME type).
     * @param applicationId the identifier of the application to which
     *            the event belongs.
     * @param eventType the application-specific type of event.
     * @param body the body of the event as a map. The map can contain nested
     *            maps for defining complex structures. When the event
     *            is not JSON, the map must contain a single key "value"
     *            with a String value.
     *
     */
    public Event(String sourceId, String syntax, String applicationId,
                 String eventType, Map<String, Object> body) {
        this(createUUID(), sourceId, syntax, applicationId, eventType, body);
    }

    /**
     * Create an event with an auto-generated event id and no event type.
     *
     * @param sourceId the id of the source of this event (normally a UUID).
     * @param syntax the syntax of the event body (a MIME type).
     * @param applicationId the identifier of the application to which
     *            the event belongs.
     * @param body the body of the event as a map. The map can contain nested
     *            maps for defining complex structures. When the event
     *            is not JSON, the map must contain a single key "value"
     *            with a String value.
     *
     */
    public Event(String sourceId, String syntax,
                 String applicationId, Map<String, Object> body) {
        this(createUUID(), sourceId, syntax, applicationId, null, body);
    }

    /**
     * Create an event with an auto-generated event id and no body.
     * The body can be set later through the methods of the class.
     *
     * @param sourceId the id of the source of this event (normally a UUID).
     * @param syntax the syntax of the event body (a MIME type).
     * @param applicationId the identifier of the application to which
     *            the event belongs.
     * @param eventType the application-specific type of event.
     *
     */
    public Event(String sourceId, String syntax,
                 String applicationId, String eventType) {
        this(createUUID(), sourceId, syntax, applicationId, eventType, null);
    }

    /**
     * Create an event with an auto-generated event id, no event type
     * and no body.
     * The body can be set later through the methods of the class.
     *
     * @param sourceId the id of the source of this event (normally a UUID).
     * @param syntax the syntax of the event body (a MIME type).
     * @param applicationId the identifier of the application to which
     *            the event belongs.
     *
     */
    public Event(String sourceId, String syntax, String applicationId) {
        this(createUUID(), sourceId, syntax, applicationId, null, null);
    }

    /**
     * Get the Event-Id field of this event.
     *
     */
    public String getEventId() {
        return eventId;
    }

    /**
     * Return the body of the event.
     *
     * @return the body of the event as a map.
     *
     */
    public Map<String, Object> getBody() {
        return body;
    }

    /**
     * Set the body of the event.
     *
     * @param bodyAsMap the body of the event as a map.
     *
     */
    public void setBody(Map<String, Object> bodyAsMap) {
        this.body = bodyAsMap;
    }

    /**
     * Set the body of the event as a String.
     *
     * @param bodyAsMap the body of the event as a String.
     *
     */
    public void setBody(String bodyAsString) {
        body = new LinkedHashMap<String, Object>();
        body.put("value", bodyAsString);
    }

    /**
     * Add an extra event header or modify an existing one.
     *
     * @param name the name of the extra header.
     * @param value the value of the extra header.
     *
     */
    public void setExtraHeader(String name, String value) {
        extraHeaders.put(name, value);
    }

    /**
     * Serialize the event as a byte array by using the configured serializer.
     *
     * @return the event serialized as a byte array.
     *
     */
    public byte[] serialize() {
        StringBuffer buffer = new StringBuffer();
        serializeHeader(buffer, "Event-Id", eventId);
        serializeHeader(buffer, "Source-Id", sourceId);
        serializeHeader(buffer, "Syntax", syntax);
        if (applicationId != null) {
            serializeHeader(buffer, "Application-Id", applicationId);
        }
        if (eventType != null) {
            serializeHeader(buffer, "Event-Type", eventType);
        }
        if (timestamp != null) {
            serializeHeader(buffer, "Timestamp", timestamp);
        }
        for (Map.Entry<String, String> entry: extraHeaders.entrySet()) {
            serializeHeader(buffer, entry.getKey(), entry.getValue());
        }
        byte[] bodyAsBytes;
        if (syntax.equals("application/json")) {
            bodyAsBytes = bodyAsJSON();
        } else {
            bodyAsBytes = body.get("value").toString().getBytes(charsetUTF8);
        }
        serializeHeader(buffer, "Body-Length",
                        String.valueOf(bodyAsBytes.length));
        buffer.append("\r\n");
        byte[] headers = buffer.toString().getBytes(charsetUTF8);
        return concatenate(headers, bodyAsBytes);
    }

    /**
     * Get a Map that represents the whole event.
     *
     * Each header is a top-level key, and the body is the value
     * of the "Body" top-level key.
     *
     * @return the whole event as a map.
     *
     */
    public Map<String, Object> toMap() {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("Event-Id", eventId);
        data.put("Source-Id", sourceId);
        data.put("Syntax", syntax);
        if (applicationId != null) {
            data.put("Application-Id", applicationId);
        }
        if (eventType != null) {
            data.put("Event-Type", eventType);
        }
        if (timestamp != null) {
            data.put("Timestamp", timestamp);
        }
        data.putAll(extraHeaders);
        if (syntax.equals("application/json")) {
            data.put("Body", body);
        } else if (body.get("value") != null) {
            data.put("Body", body.get("value").toString());
        } else {
            data.put("Body", "");
        }
        return data;
    }

    /**
     * Create and return a new UUID.
     *
     */
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

    private byte[] bodyAsJSON() {
        Gson gson = new Gson();
        return gson.toJson(body).getBytes(charsetUTF8);
    }

    public static void main(String[] args) throws IOException {
        Event event = new Event(createUUID(),
                                "text/plain", "Ztreamy-test",
                                "Test event");
        event.setBody("Test body.");
        System.out.write(event.serialize());
    }

}
