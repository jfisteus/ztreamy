package ztreamy;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import com.google.gson.Gson;

/**
 * Event serializer for the JSON serialization.
 *
 */
public class JSONSerializer implements Serializer {

    private Gson gson;

    private static Charset charsetUTF8 = Charset.forName("UTF-8");

    /**
     * Create a new instance of the serializer.
     *
     */
    public JSONSerializer() {
        gson = new Gson();
    }

    /**
     * Return the media type for the serialized data (e.g. for the
     * Content-Type header of the HTTP response).
     *
     * @return the media type.
     *
     */
    public String contentType() {
        return "application/json";
    }

    /**
     * Return the serialization of a single event.
     *
     * @return a byte array with the serialized data.
     *
     */
    public byte[] serialize(Event event) {
        return gson.toJson(event.toMap()).getBytes(charsetUTF8);
    }

    /**
     * Return the serialization of an array of events.
     *
     * @return a byte array with the serialized data.
     *
     */
    public byte[] serialize(Event[] events) {
        List<Map<String, Object>> maps = new ArrayList<Map<String, Object>>();
        for (Event event: events) {
            maps.add(event.toMap());
        }
        return gson.toJson(maps).getBytes(charsetUTF8);
    }

    public static void main(String[] args) throws IOException {
        Event event = new Event(Event.createUUID(),
                                "text/plain", "Ztreamy-test",
                                "Test event");
        event.setBody("Test body.");
        Serializer serializer = new JSONSerializer();
        System.out.write(serializer.serialize(event));
    }
}
