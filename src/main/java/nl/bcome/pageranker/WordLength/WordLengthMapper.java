package nl.bcome.pageranker.WordLength;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


class WordLengthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\W");
        for (String s : tokens) {
            int length = s.length();
            if (length > 0) {
                context.write(new Text(String.valueOf(length)), new IntWritable(1));
            }
        }
    }
}