package main.MapReducePagerank;

import main.MapReducePagerank.StepOne.SortMapper;
import main.MapReducePagerank.StepOne.SortReducer;
import main.MapReducePagerank.StepTwo.CalculateMapper;
import main.MapReducePagerank.StepTwo.CalculateReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NodeJob {

    public static void main(String[] args) throws Exception {
        NodeJob nodeJob = new NodeJob();
        nodeJob.getNodesSorter("input", "output/run-0");


        nodeJob.getCalculator("output/run-0", "output/run-1");
        nodeJob.getCalculator("output/run-1", "output/run-2");

    }

    //De eerste MapReduce optie

    //Zorgt dat alle links die naar elkaar verwijzen bij elkaar
    //komen te staan en dat deze een waarde krijgen
    private void getNodesSorter(String input, String output) throws Exception {
        Job job = new Job();
        job.setJarByClass(NodeJob.class);

        //Hier volgt het input path voor de eerste mapper
        FileInputFormat.addInputPath(job, new Path(input));

        //Hier volgt het output path voor de eerste reducer
        System.err.println("Output was sent to directory " + output);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    private void getCalculator(String input, String output) throws Exception {
        Job job = new Job();
        job.setJarByClass(NodeJob.class);

        //Hier volgt het input path voor de eerste mapper
        FileInputFormat.addInputPath(job, new Path(input));

        //Hier volgt het output path voor de eerste reducer
        System.err.println("Output was sent to directory " + output);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(CalculateMapper.class);
        job.setReducerClass(CalculateReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}