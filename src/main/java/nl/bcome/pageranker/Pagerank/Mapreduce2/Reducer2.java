package nl.bcome.pageranker.Pagerank.Mapreduce2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        //Elementen voor de formule
        
        String receivers = "";
        float a = 0;
        int g = 5;
        float PmCm = 0;


        for (Text v : values) {

            String invertedInput = v.toString();

            if(invertedInput.contains("+")){
                StringBuilder sb = new StringBuilder(invertedInput);
                sb.deleteCharAt(0);
                receivers = sb.toString();
            }
            else {

                String[] splitInvertedInfo = invertedInput.split("\\s");
                float Pm = Float.valueOf(splitInvertedInfo[1]);
                int Cm = Integer.valueOf(splitInvertedInfo[2]);

                PmCm += (Pm / Cm);
            }
        }


        double Pn = a * (1 / g) + (1 - a) * (PmCm);

        context.write(key, new Text(String.valueOf(Pn) + " " + receivers));
    }
}