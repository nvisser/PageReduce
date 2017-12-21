package nl.bcome.pageranker.Pagerank.Mapreduce2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = value.toString().split("\\s");
        String senders = input[0];

        String receivers = input[2];
        context.write(new Text(senders), new Text("+" + receivers));
        String[] receiverSplit = receivers.split(",");

        int receiverAmount = receiverSplit.length;

        for (int i = 0; i < receiverAmount; i++) {
           context.write(new Text(receiverSplit[i]), new Text(senders + " " +
                    input[1] + " " + input[2].split(",").length));

        }
    }
}
