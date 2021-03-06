package nl.bcome.pageranker.Pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Random;


public class PageJob {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(PageJob.class);
        /*
         * Volgende twee regels bepalen waar input gelezen wordt: input van cmd-line argument #1, output naar cmd-line argument #2.
         * Bijvoorbeeld: specificeer op de command-line "myinput" "myoutput" en de files in "myinput" worden gelezen en output
         * wordt weggeschreven naar "myoutput".
         * Let op: Hadoop geeft een error als je probeert naar een bestaande output-directory te schrijven, dus maak die
         * of uniek voor elke keer (evt. programmatisch) of verwijder de output-directory iedere keer.
         */
        FileInputFormat.addInputPath(job, new Path("input/input-page"));

        String pathname;
        /*
         * uncomment the following code to append a random number (0-9999) to the output dir each time.
         */
        pathname = "" + "output" + "." + new Random().nextInt(9999);
        // System.err.println("Output was sent to directory "+pathname);

        FileOutputFormat.setOutputPath(job, new Path(pathname));

        job.setMapperClass(PageMapper.class);
        job.setReducerClass(PageReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }}
