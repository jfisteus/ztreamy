package ztreamy;

/**
 * Event serializer for the Ztreamy serialization.
 *
 */
public class ZtreamySerializer implements Serializer {

    /**
     * Return the media type for the serialized data (e.g. for the
     * Content-Type header of the HTTP response).
     *
     * @return the media type.
     *
     */
    public String contentType() {
        return "application/ztreamy-event";
    }

    /**
     * Return the serialization of a single event.
     *
     * @return a byte array with the serialized data.
     *
     */
    public byte[] serialize(Event event) {
        return event.serialize();
    }

    /**
     * Return the serialization of an array of events.
     *
     * @return a byte array with the serialized data.
     *
     */
    public byte[] serialize(Event[] events) {
        byte[][] serializations = new byte[events.length][];
        int totalLength = 0;
        for (int i = 0; i < events.length; i++) {
            serializations[i] = events[i].serialize();
            totalLength += serializations[i].length;
        }
        byte[] all = new byte[totalLength];
        int pos = 0;
        for (byte[] data: serializations) {
            System.arraycopy(data, 0, all, pos, data.length);
            pos += data.length;
        }
        return all;
    }
}
