import java.net.HttpURLConnection;
import java.net.URL;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;

public class Publisher {

    private URL serverURL;
    private String logFileName;

    public Publisher(URL serverURL) {
        this(serverURL, null);
    }

    public Publisher(URL serverURL, String logFileName) {
        this.serverURL = serverURL;
        this.logFileName = logFileName;
    }

    public int publish(Event[] events) throws IOException {
        HttpURLConnection con = (HttpURLConnection) serverURL.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/ztreamy-event");
        con.setDoOutput(true);
        OutputStream out = con.getOutputStream();
        OutputStream log = null;
        if (logFileName != null) {
            log = new FileOutputStream(logFileName, true);
        }
        for (Event event: events) {
            byte[] data = event.serialize();
            out.write(data);
            if (logFileName != null) {
                log.write(data);
            }
        }
        out.close();
        if (logFileName != null) {
            log.close();
        }
        return con.getResponseCode();
    }

    public int publish(Event event) throws IOException {
        return publish(new Event[] {event});
    }

    public static void main(String[] args)
        throws IOException, InterruptedException {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Error: one command-line parameter expected");
            System.err.println("java Publisher <stream publication URI> <number of events> [data log file]");
            System.exit(1);
        }
        Publisher publisher;
        if (args.length == 2) {
            publisher = new Publisher(new URL(args[0]));
        } else {
            publisher = new Publisher(new URL(args[0]), args[2]);
        }
        int numEvents = Integer.parseInt(args[1]);
        String sourceId = Event.createUUID();
        for (int i = 0; i < numEvents; i++) {
            int result = publisher.publish(new TestEvent(sourceId));
            if (result == 200) {
                System.out.println("An event just just been sent to the server");
            } else {
                System.out.println("The server responded with error " + result);
            }
            Thread.sleep(5000);
        }
    }
}
