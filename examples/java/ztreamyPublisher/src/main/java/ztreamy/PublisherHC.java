package ztreamy;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPOutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.HttpHost;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.util.EntityUtils;


/**
 * Send events to a server to be published in a Ztreamy stream.
 *
 * This implementation uses the Apache HTTP Components client library.
 *
 */
public class PublisherHC extends Publisher {

    private HttpClientContext context;
    private HttpClientConnectionManager manager;
    private HttpRoute route;
    private HttpClientConnection connection;
    private boolean connected;


    /**
     * Create a new instance for the given stream URL.
     * Events are serialized with the Ztreamy serialization.
     *
     * @param serverURL the URL of the "publish" controller of the stream,
     *                  e.g. "http://localhost:9000/events/stream/publish".
     *
     */
    public PublisherHC(URL serverURL) {
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
    public PublisherHC(URL serverURL, Serializer serializer) {
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
    public PublisherHC(URL serverURL, String logFileName) {
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
    public PublisherHC(URL serverURL, String logFileName,
                       Serializer serializer) {
        super(serverURL, logFileName, serializer);
        // Based on https://hc.apache.org/httpcomponents-client-ga/
        //                              tutorial/html/connmgmt.html
        context = HttpClientContext.create();
        manager = new BasicHttpClientConnectionManager();
        route = new HttpRoute(new HttpHost(serverURL.getHost(),
                                           serverURL.getPort()));
        ConnectionRequest connReq = manager.requestConnection(route, null);
        try {
            connection = connReq.get(3, TimeUnit.SECONDS);
            connected = true;
        } catch (InterruptedException | ExecutionException |
                 ConnectionPoolTimeoutException e) {
            e.printStackTrace();
            connected = false;
        }
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
    @Override
    public int publish(Event[] events, boolean compress) throws IOException {
        if (!connected) {
            throw new IOException("The connection hasn't been established");
        }
        if (!connection.isOpen()) {
            // establish connection based on its route info
            manager.connect(connection, route, 5000, context);
            // and mark it as route complete
            manager.routeComplete(connection, route, context);
        }
        BasicHttpEntityEnclosingRequest req =
            new BasicHttpEntityEnclosingRequest("POST", serverURL.getFile());
        req.setHeader("Host", serverURL.getHost());
        req.setHeader("Content-Type", serializer.contentType());
        if (compress) {
            req.setHeader("Content-Encoding", "gzip");
        }
        OutputStream log = null;
        if (logFileName != null) {
            log = new FileOutputStream(logFileName, true);
        }
        byte[] data;
        if (events.length == 1) {
            data = serializer.serialize(events[0]);
        } else {
            data = serializer.serialize(events);
        }
        InputStream bodyIS;
        if (compress) {
            ByteArrayOutputStream compressedOS = new ByteArrayOutputStream();
            GZIPOutputStream gzipOS = new GZIPOutputStream(compressedOS);
            gzipOS.write(data);
            gzipOS.close();
            data = compressedOS.toByteArray();
        }
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream(data));
        req.setEntity(entity);
        req.setHeader("Content-Length", "" + data.length);
        int result;
        try {
            connection.sendRequestHeader(req);
            connection.sendRequestEntity(req);
            HttpResponse response = connection.receiveResponseHeader();
            connection.receiveResponseEntity(response);
            result = response.getStatusLine().getStatusCode();
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null) {
                EntityUtils.consume(responseEntity);
            }
        } catch (HttpException e) {
            throw new IOException(e);
        }

        if (logFileName != null) {
            log.write(data);
            log.close();
        }
        return result;
    }

    /**
     * Closes this publisher.
     *
     */
    @Override
    public void close() {
        manager.shutdown();
    }
}
