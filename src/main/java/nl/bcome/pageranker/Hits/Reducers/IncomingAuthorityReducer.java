package nl.bcome.pageranker.Hits.Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class IncomingAuthorityReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double theAuth = 0;
        for (Text incomingNeighbor : values) {
            StringTokenizer x = new StringTokenizer(incomingNeighbor.toString());
            String page = x.nextToken();

            double auth = Double.parseDouble(x.nextToken());
            double hub = Double.parseDouble(x.nextToken());

            theAuth += hub;
            System.err.println("Auth score for " + key.toString() + " is now at " + theAuth);
        }
        context.write(key, new DoubleWritable(theAuth));
    }
}