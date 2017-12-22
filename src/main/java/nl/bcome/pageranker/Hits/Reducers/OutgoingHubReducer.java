package nl.bcome.pageranker.Hits.Reducers;

import com.google.gson.Gson;
import nl.bcome.pageranker.Hits.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OutgoingHubReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text p, Iterable<Text> incomingNeighbors, Context context) throws IOException, InterruptedException {
        double theHub = 0; // then update all hub values
        double norm = 0;
        double allHub = 0;
        double pAuth = 0;
        double finalAuth = 0;

        for (Text q : incomingNeighbors) { // p.outgoingNeighbors is the set of pages that p links to

            if (p.toString().charAt(0) != '+') {

                Node node = new Gson().fromJson(q.toString(), Node.class);

                double auth = node.getAuth();
                double hub = node.getHub();
                allHub += hub;

                // Calculate the hub score
                theHub += auth;
                System.err.println("Hub score for " + p.toString() + " is now at " + theHub);
            } else {
                pAuth = Double.parseDouble(q.toString());
            }


        }

        // ??
        finalAuth = pAuth + allHub;
        // Unfortunately norm didn't make the cut.
        norm += theHub * theHub; // calculate the sum of the squared auth values to normalise
//        System.err.println("Norm for " + p.toString() + " is now " + norm);

        String result = String.format("%f %s %f",finalAuth, p, theHub);
        if(p.toString().charAt(0) != '+') {
            context.write(p, new Text(result));
        }
        // At some point we should do <code>norm = sqrt(norm)</code> - But it's not in this reducer. Where then?
        // Perhaps the next mapper?
    }
}