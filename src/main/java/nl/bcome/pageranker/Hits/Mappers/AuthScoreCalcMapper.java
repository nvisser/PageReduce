package nl.bcome.pageranker.Hits.Mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AuthScoreCalcMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer x = new StringTokenizer(value.toString());

        // Incoming
        // Page hub auth norm
//        String hubAuth = String.format("%s %d %d %d", tokens[0], 1, 1, 0);
//        context.write(new Text(tokens[1]), new Text(hubAuth));
    }
}