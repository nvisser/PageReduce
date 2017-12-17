package nl.bcome.pageranker.Join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("MAP: Key=" + Key.toString() + " - Value="+ value.toString());
//        String[] tokens = value.toString().split("\\s");
//        for (String s : tokens) {
            context.write(new Text("key"), new Text("value"));
//        }
    }
}