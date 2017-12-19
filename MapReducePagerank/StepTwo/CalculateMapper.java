package main.MapReducePagerank.StepTwo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class CalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        //Pak de value die uit het tekstbestand komt
        String[] tokens = value.toString().split("\\s");

        //Pak alle links waar deze pagina naar toe verwijst en maak daar een list van
        String nodesToStartNode = tokens[2];
        String startPagina = tokens[0];
        String[] linksFromPageList = nodesToStartNode.split(",");

        //Aantal pagina's
        int aantalLinks = linksFromPageList.length;

        for (int i = 0; i < aantalLinks; i++) {

            //System.out.println(linksFromPageList[i] + ":" + startPagina + " " +
              //      tokens[1] + " " + Arrays.toString(tokens[2].split(",")));

           context.write(new Text(linksFromPageList[i]), new Text(startPagina + " " +
                    tokens[1] + " " + tokens[2].split(",").length));

           // context.write(new Text(startPagina), new Text(linksFromPageList[i] + " " +
             //       tokens[1] + " " + tokens[2].split(",").length));

        }
    }
}
