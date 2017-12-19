package nl.bcome.pageranker.Join;

import nl.bcome.pageranker.Join.models.Customer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class JoinReducer extends Reducer<IntWritable, Customer, Text, IntWritable> {
    public void reduce(Text key, Iterable<Customer> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
//        for (IntWritable i : values) {
//            sum += i.get();
//        }
        context.write(key, new IntWritable(sum));
    }
}