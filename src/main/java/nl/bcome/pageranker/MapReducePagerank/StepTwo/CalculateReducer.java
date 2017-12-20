package nl.bcome.pageranker.MapReducePagerank.StepTwo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CalculateReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String nodes = "";
        /*
        Zet formule variabelen van de formule: P(n) = a(1/|G|) + (1-a)(∑ P(m)/C(m))

        a = voor de kans dat je een node een sprong maakt (in ons geval 0)
        g = het aantal nodes in het complete netwerk
        PmCm = de waarde van de pagerank die er word gegeven door de nodes erom heen
            Meer informatie in de for-loop hieronder
        */

        float a = 0;
        int g = 5;
        float PmCm = 0;

        /*
        Bereken de (∑ P(m)/C(m)) gedeelte van de formule
        Ga elke node langs en controlleer en bereken twee dingen:

        1. de Pm waarde oftewel de Pagerank die er al was bij die node
        2. de Cm waarde oftewel het aantal nodes dat er uitgaande is

        Zodra je deze twee dingen weet deel je ze door elkaar en tel je ze op (optelsom ∑)
        */
        for (Text v : values) {
            String nodeAllInfo = v.toString();
            System.out.println(key + ":" +v.toString());

            String[] info = nodeAllInfo.split("\\s");

            float Pm = Float.valueOf(info[1]);
            int Cm = Integer.valueOf(info[2]);

            PmCm += (Pm / Cm);

            nodes += info[0] + ",";
        }

        //Formule PageRank
        //P(n) = a(1/|G|) + (1-a)(∑ P(m)/C(m))
        double Pn = a * (1 / g) + (1 - a) * (PmCm);

        context.write(key, new Text(String.valueOf(Pn) + " " + nodes));
    }
}