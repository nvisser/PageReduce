package nl.bcome.pageranker.Hits.Mappers;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import nl.bcome.pageranker.Hits.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class OutgoingHubMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer x = new StringTokenizer(value.toString());
        String incoming = x.nextToken();

        String output = x.nextToken();

        // This is a bit of json fuckery. It's really fucky to split on : or ,
        // so we have to improvise a bit. I swear there could be an easier way to do this
        // but it's not like we have time to find out how
        String f = output.split("}:\\{")[0] + "}";
        Node first = new Gson().fromJson(f, Node.class);


        output = "[{" + output.split("}:\\{")[1]+ "]";
        JsonArray nodes = new JsonParser().parse(output).getAsJsonArray();
        for (JsonElement n : nodes) {
            Node node = new Gson().fromJson(n, Node.class);
            node.getHub();
        }


//        for(String tok : output.split(",")) {
//            Node n = new Gson().fromJson(x.nextToken(), Node.class);
//            n.getHub();
//        }
//        Node n = new Gson().fromJson(x.nextToken(), Node.class);

        // Incoming
        //                                            Page, hub, auth, norm
//        String hubAuth = String.format("%s %d %d %d", tokens[0], 1, 1, 0);
//        context.write(new Text(tokens[0]), new Text(hubAuth));
    }
}