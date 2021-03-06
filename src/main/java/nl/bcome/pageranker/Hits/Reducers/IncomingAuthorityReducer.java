package nl.bcome.pageranker.Hits.Reducers;

import com.google.gson.Gson;
import nl.bcome.pageranker.Hits.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IncomingAuthorityReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text p, Iterable<Text> incomingNeighbors, Context context) throws IOException, InterruptedException {
        double theAuth = 0; // update all authority values first
        double norm = 0;
        StringBuilder relations = new StringBuilder();
        for (Text x : incomingNeighbors) { // p.incomingNeighbors is the set of pages that link to p
            Node n = new Gson().fromJson(x.toString(), Node.class);
            // Calculate the auth score
            theAuth += n.getHub();
            System.err.println("Auth score for " + p.toString() + " is now at " + theAuth);
            relations.append(x.toString()).append(",");
        }
        // Quite possibly the weirdest way to truncate that extra ,
        relations.setLength(relations.length() - 1);

        norm += theAuth * theAuth; // calculate the sum of the squared auth values to normalise

        // Make an object with the auth score and the initial hub score
        Node pn = new Node(p.toString(), theAuth, 1);
        context.write(p, new Text(new Gson().toJson(pn, Node.class) + ":" + relations));
    }
}