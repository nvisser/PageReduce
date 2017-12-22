package nl.bcome.pageranker.Hits.Mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AuthScoreCalcMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer x = new StringTokenizer(value.toString());
        String page = x.nextToken();
        double auth = Double.parseDouble(x.nextToken());
        double norm = Double.parseDouble(x.nextToken());

        norm = Math.sqrt(norm);

        auth = auth / norm; // normalise the auth values

        String authScore = String.format("%f %f", auth, norm);
        context.write(new Text(page), new Text(authScore));
    }
}