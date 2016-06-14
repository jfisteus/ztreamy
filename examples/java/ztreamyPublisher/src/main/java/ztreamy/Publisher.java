package ztreamy;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

/**
 * Send events to a server to be published in a Ztreamy stream.
 *
 */
public class Publisher {

    protected URL serverURL;
    protected String logFileName;
    protected Serializer serializer;

    /**
     * Create a new instance for the given stream URL.
     * Events are serialized with the Ztreamy serialization.
     *
     * @param serverURL the URL of the "publish" controller of the stream,
     *                  e.g. "http://localhost:9000/events/stream/publish".
     *
     */
    public Publisher(URL serverURL) {
        this(serverURL, null, new ZtreamySerializer());
    }

    /**
     * Create a new instance for the given stream URL
     * and a configurable event serializer.
     *
     * @param serverURL the URL of the "publish" controller of the stream,
     *                  e.g. "http://localhost:9000/events/stream/publish".
     * @param serializer the serializer to be used to serialize the events.
     *
     */
    public Publisher(URL serverURL, Serializer serializer) {
        this(serverURL, null, serializer);
    }

    /**
     * Create a new instance for the given stream URL.
     * Events are serialized with the Ztreamy serialization.
     * The events are logged into a file.
     *
     * @param serverURL the URL of the "publish" controller of the stream,
     *                  e.g. "http://localhost:9000/events/stream/publish".
     * @param logFileName the name of the file to which the events
     *                    will be logged (appended to the end of the file).
     *
     */
    public Publisher(URL serverURL, String logFileName) {
        this(serverURL, logFileName, new ZtreamySerializer());
    }

    /**
     * Create a new instance for the given stream URL
     * and a configurable event serializer.
     * The events are logged into a file.
     *
     * @param serverURL the URL of the "publish" controller of the stream,
     *                  e.g. "http://localhost:9000/events/stream/publish".
     * @param serializer the serializer to be used to serialize the events.
     * @param logFileName the name of the file to which the events
     *                    will be logged (appended to the end of the file).
     *
     */
    public Publisher(URL serverURL, String logFileName, Serializer serializer) {
        this.serverURL = serverURL;
        this.logFileName = logFileName;
        this.serializer = serializer;
    }

    /**
     * Publish several events through a single HTTP POST request.
     *
     * @param events the array of events to be published.
     * @return the response code of the server (200 if everything was ok).
     *
     */
    public int publish(Event[] events) throws IOException {
        return publish(events, false);
    }

    /**
     * Publish several events through a single HTTP POST request.
     * Optionally use GZIP for compressing the events.
     *
     * @param events the array of events to be published.
     * @param compress true for GZIP compression, false for no compression.
     * @return the response code of the server (200 if everything was ok).
     *
     */
    public int publish(Event[] events, boolean compress) throws IOException {
        HttpURLConnection con = (HttpURLConnection) serverURL.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", serializer.contentType());
        if (compress) {
            con.setRequestProperty("Content-Encoding", "gzip");
        }
        con.setDoOutput(true);
        OutputStream out = con.getOutputStream();
        OutputStream log = null;
        if (logFileName != null) {
            log = new FileOutputStream(logFileName, true);
        }
        if (compress) {
            out = new GZIPOutputStream(out);
        }
        byte[] data;
        if (events.length == 1) {
            data = serializer.serialize(events[0]);
        } else {
            data = serializer.serialize(events);
        }
        out.write(data);
        if (logFileName != null) {
            log.write(data);
        }
        out.close();
        if (logFileName != null) {
            log.close();
        }
        int result = con.getResponseCode();
        // Consume the response body:
        con.getInputStream().close();
        return result;
    }

    /**
     * Publish a single event through an HTTP POST request.
     *
     * @param event the event to be published.
     * @return the response code of the server (200 if everything was ok).
     *
     */
    public int publish(Event event) throws IOException {
        return publish(event, false);
    }

    /**
     * Publish a single event through an HTTP POST request.
     * Optionally use GZIP for compressing the events.
     *
     * @param event the event to be published.
     * @param compress true for GZIP compression, false for no compression.
     * @return the response code of the server (200 if everything was ok).
     *
     */
    public int publish(Event event, boolean compress) throws IOException {
        return publish(new Event[] {event}, compress);
    }

    /**
     * Closes this publisher.
     *
     */
    public void close() {
        // Nothing to do, no resources are allocated
    }
}
