package nl.bcome.pageranker.Hits.Reducers;

import com.google.gson.Gson;
import nl.bcome.pageranker.Hits.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class IncomingAuthorityReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text p, Iterable<Text> incomingNeighbors, Context context) throws IOException, InterruptedException {
        double theAuth = 0; // update all authority values first
        double norm = 0;
        for (Text x : incomingNeighbors) { // p.incomingNeighbors is the set of pages that link to p
            Node n = new Gson().fromJson(x.toString(), Node.class);
            theAuth += n.getHub();
            System.err.println("Auth score for " + p.toString() + " is now at " + theAuth);
        }

        norm += theAuth * theAuth; // calculate the sum of the squared auth values to normalise

        Node pn = new Node(p.toString(), theAuth, 1);
        context.write(p, new Text(new Gson().toJson(pn, Node.class)));
    }
}