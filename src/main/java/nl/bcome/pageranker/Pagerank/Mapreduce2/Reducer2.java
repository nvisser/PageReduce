package nl.bcome.pageranker.Pagerank.Mapreduce2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        //Elementen voor de formule
        
        String receivers = "";
        float a = 0; //kans dat iemand via de random jump probability op een node uitkomt
        int g = 5; //kans dat iemand via een link op een node uitkomt
        float PmCm = 0; //PageRank in-nodes


        for (Text v : values) {

            //de resultaten van de mapper in een string veranderen
            String invertedInput = v.toString();

            // er wordt gekeken of de string een plus bevat, zo ja wordt deze aan het einde van de file output toegevoegd
            // dit is zo gedaan, zodat de index weer omgedraait kan worden bij de volgende ronde en de berekening van PmCm correct blijft
            if(invertedInput.contains("+")){
                StringBuilder sb = new StringBuilder(invertedInput);
                sb.deleteCharAt(0);
                receivers = sb.toString();
            }
            //berekening van PmCm voor de formule
            else {

                String[] splitInvertedInfo = invertedInput.split("\\s");
                float Pm = Float.valueOf(splitInvertedInfo[1]);
                int Cm = Integer.valueOf(splitInvertedInfo[2]);

                PmCm += (Pm / Cm);
            }
        }

        // Page rank formule
        double answer = a * (1 / g) + (1 - a) * (PmCm);

        context.write(key, new Text(String.valueOf(answer) + " " + receivers));
    }
}