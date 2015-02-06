import java.net.URL;
import java.net.HttpURLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class Consumer {

    private URL url;

    public Consumer(URL streamURL) {
        url = streamURL;
    }

    public void run() throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(con.getInputStream()));
        String line;
        try {
            while (true) {
                line = reader.readLine();
                if (line != null) {
                    System.out.println(line);
                } else {
                    break;
                }
            }
        } finally {
            con.disconnect();
        }
    }

    public static void main(String[] args)
        throws IOException, InterruptedException {
        if (args.length != 1) {
            System.err.println("Error: one command-line parameter expected");
            System.err.println("java Consumer <stream URI>");
            System.exit(1);
        }
        Consumer consumer = new Consumer(new URL(args[0]));
        consumer.run();
    }
}
