package nl.bcome.pageranker.Hits;

import nl.bcome.pageranker.Hits.Mappers.IncomingAuthorityMapper;
import nl.bcome.pageranker.Hits.Mappers.OutgoingHubMapper;
import nl.bcome.pageranker.Hits.Reducers.IncomingAuthorityReducer;
import nl.bcome.pageranker.Hits.Reducers.OutgoingHubReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hits {

    public static void main(String[] args) throws Exception {
        // Calculate incoming neighbors
        incomingNeighbors();
        outgoingNeighbors();
    }

    /**
     * Calculate incoming neighbors
     *
     * @throws Exception Oopsie.
     */
    private static void incomingNeighbors() throws Exception {
        Job job = new Job();
        job.setJarByClass(Hits.class);

        FileInputFormat.addInputPath(job, new Path("input/input-hits/step1"));
        FileOutputFormat.setOutputPath(job, new Path("output/output-hits/IncomingNeighbors"));// + new Random().nextInt(9999)));

        job.setMapperClass(IncomingAuthorityMapper.class);
        job.setReducerClass(IncomingAuthorityReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }

    /**
     * Calculate outgoing neighbors
     *
     * @throws Exception Oopsie.
     */
    private static void outgoingNeighbors() throws Exception {
        Job job = new Job();
        job.setJarByClass(Hits.class);

        FileInputFormat.addInputPath(job, new Path("input/input-hits/step1"));
        FileOutputFormat.setOutputPath(job, new Path("output/output-hits/OutgoingNeighbors"));// + new Random().nextInt(9999)));

        job.setMapperClass(OutgoingHubMapper.class);
        job.setReducerClass(OutgoingHubReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }
}