public class ZtreamySerializer implements Serializer {

    public String contentType() {
        return "application/ztreamy-event";
    }

    public byte[] serialize(Event event) {
        return event.serialize();
    }

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
