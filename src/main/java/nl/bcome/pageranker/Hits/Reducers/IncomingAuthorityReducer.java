package nl.bcome.pageranker.Hits.Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class IncomingAuthorityReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text p, Iterable<Text> incomingNeighbors, Context context) throws IOException, InterruptedException {
        double theAuth = 0;
        double norm = 0;
        for (Text q : incomingNeighbors) {
            StringTokenizer x = new StringTokenizer(q.toString());
            String page = x.nextToken();

            double auth = Double.parseDouble(x.nextToken());
            double hub = Double.parseDouble(x.nextToken());

            theAuth += hub;
            System.err.println("Auth score for " + p.toString() + " is now at " + theAuth);
        }
        norm = Math.sqrt(theAuth);
        String result = String.format("%s %f %f", p, theAuth, norm);
        context.write(p, new Text(result));
    }
}