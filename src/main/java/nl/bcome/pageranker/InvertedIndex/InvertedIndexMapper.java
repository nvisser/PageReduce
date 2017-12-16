package nl.bcome.pageranker.InvertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");
        for (String s : tokens) {
            if (s.equals(tokens[0]))
                continue;
            context.write(new Text(s), new Text(tokens[0]));
        }
    }
}