package nl.bcome.pageranker.Hits.Reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class OutgoingHubReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text p, Iterable<Text> incomingNeighbors, Context context) throws IOException, InterruptedException {
        double theHub = 0; // then update all hub values
        double norm = 0;
        for (Text q : incomingNeighbors) { // p.outgoingNeighbors is the set of pages that p links to
            StringTokenizer x = new StringTokenizer(q.toString());
            String page = x.nextToken();

            double auth = Double.parseDouble(x.nextToken());
            double hub = Double.parseDouble(x.nextToken());

            theHub += auth;
            System.err.println("Hub score for " + p.toString() + " is now at " + theHub);
        }
        norm += theHub * theHub; // calculate the sum of the squared auth values to normalise
        System.err.println("Norm for " + p.toString() + " is now " + norm);

        String result = String.format("%f %f", theHub, norm);
        context.write(p, new Text(result));
        // At some point we should do <code>norm = sqrt(norm)</code> - But it's not in this reducer. Where then?
        // Perhaps the next mapper?
    }
}