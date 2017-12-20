package nl.bcome.pageranker.Pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class PageReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder websites = new StringBuilder();
        for (Text i : values) {
            websites.append(" ").append(i);
        }
        context.write(key, new Text(websites.toString()));
    }
}
