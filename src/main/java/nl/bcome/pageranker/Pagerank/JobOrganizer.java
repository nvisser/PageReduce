package nl.bcome.pageranker.Pagerank;

import nl.bcome.pageranker.Pagerank.Mapreduce1.*;
import nl.bcome.pageranker.Pagerank.Mapreduce2.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class JobOrganizer {

    public static void main(String[] args) throws Exception {

        //Verwijdert de output map als die al bestaat

        File output = new File("output");
        FileUtils.deleteDirectory(output);

        JobOrganizer jobOrganizer = new JobOrganizer();
        jobOrganizer.startIt(10, jobOrganizer);


    }

    private void  startIt(int amount, JobOrganizer job) throws Exception{

        //roept de eerste mapreduce aan
        job.Prepare("input/input-pagerank", "output/ronde1");

        //roept de tweede mapreduce aan
        for(int i = 1; i != amount; i++){
            job.getCalculator("output/ronde" + i, "output/ronde" + (i+1));
        }
    }

    // voor pagerank hebben we de mapreduce techniek gesplitst voor 2 taken

    //De eerste map reduce sorteert de nodes dus danig dat je kan zien wat de node voor waarde heeft en waar die het naar toe stuurt
    //Er zijn functie voor gemaakt zodat je de input makkelijker kan specificeren

    private void Prepare(String input, String output) throws Exception {
        Job job = new Job();
        job.setJarByClass(JobOrganizer.class);

        //input en output gespecificeerd
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // specificeren welke classen de mapper en reducer zijn + input en output format
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    private void getCalculator(String input, String output) throws Exception {
        Job job = new Job();
        job.setJarByClass(JobOrganizer.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}