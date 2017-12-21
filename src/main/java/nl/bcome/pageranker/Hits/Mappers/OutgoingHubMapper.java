package nl.bcome.pageranker.Hits.Mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class OutgoingHubMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");

        // Incoming
        // Page hub auth norm
        String hubAuth = String.format("%s %d %d %d", tokens[0], 1, 1, 0);
        context.write(new Text(tokens[0]), new Text(hubAuth));
    }
}