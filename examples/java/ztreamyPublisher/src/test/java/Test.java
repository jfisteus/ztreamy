import java.io.IOException;
import java.net.URL;

import ztreamy.Event;
import ztreamy.JSONSerializer;
import ztreamy.Publisher;
import ztreamy.Serializer;

public class Test {

    public static void main(String[] args) throws IOException,
                                                  InterruptedException {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Error: one command-line parameter expected");
            System.err.println("java Publisher <stream publication URI> "
                               + "<number of events> [data log file]");
            System.exit(1);
        }
        Publisher publisher;
        Serializer serializer = new JSONSerializer();
        if (args.length == 2) {
            publisher = new Publisher(new URL(args[0]), serializer);
        } else {
            publisher = new Publisher(new URL(args[0]), args[2], serializer);
        }
        int numEvents = Integer.parseInt(args[1]);
        String sourceId = Event.createUUID();
        for (int i = 0; i < numEvents; i++) {
            int result = publisher.publish(new TestEvent(sourceId));
            if (result == 200) {
                System.out
                    .println("An event just just been sent to the server");
            } else {
                System.out.println("The server responded with error " + result);
            }
            Thread.sleep(15000);
        }
    }
}
