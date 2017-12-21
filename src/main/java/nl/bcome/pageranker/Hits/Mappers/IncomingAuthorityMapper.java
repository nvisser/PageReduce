package nl.bcome.pageranker.Hits.Mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class IncomingAuthorityMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");

        // Incoming
        // Hubs and auths start at one
        String hubAuth = String.format("%s %d %d", tokens[0], 1, 1);
        context.write(new Text(tokens[1]), new Text(hubAuth));
    }
}