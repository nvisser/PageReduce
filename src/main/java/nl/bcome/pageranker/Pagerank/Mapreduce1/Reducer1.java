package nl.bcome.pageranker.Pagerank.Mapreduce1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //voeg de start waardes 0.2 per rij toe
        StringBuilder sb = new StringBuilder(0.2 + " ");
        for (Text v : values) {
            sb.append(v).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);

        context.write(key, new Text(sb.toString()));
    }
}