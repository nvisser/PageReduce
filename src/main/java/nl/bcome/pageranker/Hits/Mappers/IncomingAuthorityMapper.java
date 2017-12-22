package nl.bcome.pageranker.Hits.Mappers;

import com.google.gson.Gson;
import nl.bcome.pageranker.Hits.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class IncomingAuthorityMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");

        // Incoming
        Node n = new Node(tokens[0], 1, 1);
        context.write(new Text(tokens[1]), new Text(
                new Gson().toJson(n, Node.class)
        ));
    }
}