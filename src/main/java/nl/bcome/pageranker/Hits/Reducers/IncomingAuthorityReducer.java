package nl.bcome.pageranker.Hits.Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class IncomingAuthorityReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    public void reduce(Text p, Iterable<Text> incomingNeighbors, Context context) throws IOException, InterruptedException {
        double theAuth = 0;
        int norm = 0;
        for (Text q : incomingNeighbors) {
            StringTokenizer x = new StringTokenizer(q.toString());
            String page = x.nextToken();

            double auth = Double.parseDouble(x.nextToken());
            double hub = Double.parseDouble(x.nextToken());

            theAuth += hub;
            System.err.println("Auth score for " + p.toString() + " is now at " + theAuth);
        }
        context.write(p, new DoubleWritable(theAuth));
    }
}