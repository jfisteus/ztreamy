package ztreamy;

/**
 * Interface for event serializers.
 *
 */
public interface Serializer {

    /**
     * Return the media type for the serialized data (e.g. for the
     * Content-Type header of the HTTP response).
     *
     * @return the media type.
     *
     */
    String contentType();

    /**
     * Return the serialization of a single event.
     *
     * @return a byte array with the serialized data.
     *
     */
    byte[] serialize(Event event);

    /**
     * Return the serialization of an array of events.
     *
     * @return a byte array with the serialized data.
     *
     */
    byte[] serialize(Event[] events);
}
