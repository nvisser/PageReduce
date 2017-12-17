package nl.bcome.pageranker.Join;

import com.google.gson.GsonBuilder;
import nl.bcome.pageranker.Join.models.Customer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Customer> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        Customer c = new GsonBuilder().create().fromJson(value.toString(), Customer.class);
        context.write(new IntWritable(c.getId()), c);
    }
}