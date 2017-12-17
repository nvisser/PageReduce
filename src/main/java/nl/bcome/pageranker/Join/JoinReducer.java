package nl.bcome.pageranker.Join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("MAP: Key=" + key.toString() + " - Value="+ values.toString());
        int sum = 0;
//        for (IntWritable i : values) {
//            sum += i.get();
//        }
        context.write(key, new IntWritable(sum));
    }
}