package nl.bcome.pageranker.Hits;

import nl.bcome.pageranker.Hits.Mappers.IncomingAuthorityMapper;
import nl.bcome.pageranker.Hits.Mappers.OutgoingHubMapper;
import nl.bcome.pageranker.Hits.Reducers.IncomingAuthorityReducer;
import nl.bcome.pageranker.Hits.Reducers.OutgoingHubReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Hits {

    public static void main(String[] args) throws Exception {
        // Calculate incoming neighbors
        incomingNeighbors("input/input-hits/step1", "output/output-hits/IncomingNeighbors");

        // Calculate outgoing neighbors (gee, the neighbors are really outgoing)
//        outgoingNeighbors("input-input-hits/step1", "output/output-hits/IncomingNeighbors");
    }

    /**
     * Calculate incoming neighbors
     *
     * @param input  Where do we take our input from
     * @param output Where to save our output
     * @throws Exception Oopsie.
     */
    private static void incomingNeighbors(String input, String output) throws Exception {
        Job job = new Job();
        job.setJarByClass(Hits.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(IncomingAuthorityMapper.class);
        job.setReducerClass(IncomingAuthorityReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

    /**
     * Calculate outgoing neighbors
     *
     * @param input  Where do we take our input from
     * @param output Where to save our output
     * @throws Exception Oopsie.
     */
    private static void outgoingNeighbors(String input, String output) throws Exception {
        Job job = new Job();
        job.setJarByClass(Hits.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(OutgoingHubMapper.class);
        job.setReducerClass(OutgoingHubReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}