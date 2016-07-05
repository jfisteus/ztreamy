import java.io.IOException;
import java.net.URL;

import ztreamy.Event;
import ztreamy.JSONSerializer;
import ztreamy.Publisher;
import ztreamy.PublisherHC;
import ztreamy.Serializer;

public class TestHC {

    public static void main(String[] args) throws IOException,
                                                  InterruptedException {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Error: one command-line parameter expected");
            System.err.println("java Publisher <stream publication URI> "
                               + "<number of events> [data log file]");
            System.exit(1);
        }
        Publisher publisher1, publisher2;
        Serializer serializer = new JSONSerializer();
        if (args.length == 2) {
            publisher1 = new PublisherHC(new URL(args[0]), serializer);
            publisher2 = new PublisherHC(new URL(args[0]), serializer);
        } else {
            publisher1 = new PublisherHC(new URL(args[0]), args[2], serializer);
            publisher2 = new PublisherHC(new URL(args[0]), args[2], serializer);
        }
        int numEvents = Integer.parseInt(args[1]);
        String sourceId1 = Event.createUUID();
        String sourceId2 = Event.createUUID();
        for (int i = 0; i < numEvents; i++) {
            int result = publisher1.publish(new TestEvent(sourceId1));
            if (result == 200) {
                System.out.println("[1] Event published");
            } else {
                System.out.println("[1] The server responded with " + result);
            }
            result = publisher2.publish(new TestEvent(sourceId2));
            if (result == 200) {
                System.out.println("[2] Event published");
            } else {
                System.out.println("[2] The server responded with " + result);
            }
            Thread.sleep(5000);
        }
        publisher1.close();
        publisher2.close();
    }
}
