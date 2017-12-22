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


        // This is actually the best way to get a json array. Don't believe me? Neither do I.
        output = "[{" + output.split("}:\\{")[1]+ "]";
        JsonArray nodes = new JsonParser().parse(output).getAsJsonArray();
        for (JsonElement n : nodes) {
            Node node = new Gson().fromJson(n, Node.class);

            context.write(new Text(node.getName()), new Text(
                    new Gson().toJson(first, Node.class)
            ));
        }
    }
}