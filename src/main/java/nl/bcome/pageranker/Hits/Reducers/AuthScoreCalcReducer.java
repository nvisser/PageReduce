package nl.bcome.pageranker.Hits.Reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class AuthScoreCalcReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text p, Iterable<Text> incomingNeighbors, Context context) throws IOException, InterruptedException {
        double theAuth = 0; // update all authority values first
        double norm = 0;
        for (Text q : incomingNeighbors) { // p.incomingNeighbors is the set of pages that link to p
            StringTokenizer x = new StringTokenizer(q.toString());
            String page = x.nextToken();

            double auth = Double.parseDouble(x.nextToken());
            double hub = Double.parseDouble(x.nextToken());

            theAuth += hub;
            System.err.println("Auth score for " + p.toString() + " is now at " + theAuth);
        }
        norm += theAuth * theAuth; // calculate the sum of the squared auth values to normalise
        System.err.println("Norm for " + p.toString() + " is now " + norm);

        String result = String.format("%f %f", theAuth, norm);
        context.write(p, new Text(result));
        // At some point we should do <code>norm = sqrt(norm)</code> - But it's not in this reducer. Where then?
        // Perhaps the next mapper?
    }
}