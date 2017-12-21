package nl.bcome.pageranker.Pagerank.Mapreduce1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {

        //splitten van alle waardes
        String[] input = value.toString().split("\\s");
        //sleutel specificeren
        String key = input[0];
        for (int i = 1; i < input.length; i++) {
            context.write(new Text(key), new Text(input[i]));
        }
    }
}
