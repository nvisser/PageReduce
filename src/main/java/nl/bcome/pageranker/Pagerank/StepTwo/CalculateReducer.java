package nl.bcome.pageranker.Pagerank.StepTwo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CalculateReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String nodes = "";


        float a = 0;
        int g = 5;
        float PmCm = 0;


        for (Text v : values) {

            String nodeAllInfo = v.toString();
            System.out.println(key + ":" +v.toString());

            if(nodeAllInfo.contains("+")){
                StringBuilder sb = new StringBuilder(nodeAllInfo);
                sb.deleteCharAt(0);
                nodes = sb.toString();
            }
            else {

                String[] info = nodeAllInfo.split("\\s");

                float Pm = Float.valueOf(info[1]);
                int Cm = Integer.valueOf(info[2]);

                PmCm += (Pm / Cm);

                //   nodes += info[0] + ",";
            }
        }

        //Formule PageRank
        //P(n) = a(1/|G|) + (1-a)(∑ P(m)/C(m))
        double Pn = a * (1 / g) + (1 - a) * (PmCm);

        context.write(key, new Text(String.valueOf(Pn) + " " + nodes));
    }
}