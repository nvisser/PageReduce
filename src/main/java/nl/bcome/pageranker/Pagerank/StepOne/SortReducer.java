package nl.bcome.pageranker.Pagerank.StepOne;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append(0.2 + " ");
        for (Text v : values) {
            sb.append(v).append(",");
        }
        sb.setLength(sb.length() - 1);
        System.out.println(key + ": " + sb.toString());
        context.write(key, new Text(sb.toString()));
    }
}