package nl.bcome.pageranker.MapReducePagerank.StepOne;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");
        String key = tokens[0];
        for (int i = 1; i < tokens.length; i++) {
            context.write(new Text(key), new Text(tokens[i]));
        }
    }
}
