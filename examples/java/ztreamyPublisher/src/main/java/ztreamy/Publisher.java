package ztreamy;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

public class Publisher {

    private URL serverURL;
    private String logFileName;
    private Serializer serializer;

    public Publisher(URL serverURL) {
        this(serverURL, null, new ZtreamySerializer());
    }

    public Publisher(URL serverURL, Serializer serializer) {
        this(serverURL, null, serializer);
    }

    public Publisher(URL serverURL, String logFileName) {
        this(serverURL, logFileName, new ZtreamySerializer());
    }

    public Publisher(URL serverURL, String logFileName, Serializer serializer) {
        this.serverURL = serverURL;
        this.logFileName = logFileName;
        this.serializer = serializer;
    }

    public int publish(Event[] events) throws IOException {
        return publish(events, false);
    }

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
        return con.getResponseCode();
    }

    public int publish(Event event) throws IOException {
        return publish(event, false);
    }

    public int publish(Event event, boolean compress) throws IOException {
        return publish(new Event[] {event}, compress);
    }
}
