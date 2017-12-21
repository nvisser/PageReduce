package nl.bcome.pageranker.Hits.Reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class ScoreCalcReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text p, Iterable<Text> entries, Context context) throws IOException, InterruptedException {
        double auth = 0, norm = 0;

        // This is the best(worst?) way to get the last entry.
        for (Text t : entries) {
            StringTokenizer x = new StringTokenizer(t.toString());
            auth = Double.parseDouble(x.nextToken());
            norm = Double.parseDouble(x.nextToken());
        }

        String result = String.format("%f %f", auth, norm);
        context.write(p, new Text(result));
    }
}