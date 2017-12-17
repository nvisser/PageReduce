package nl.bcome.pageranker.Join;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import nl.bcome.pageranker.Join.models.Customer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        Customer c = new GsonBuilder().create().fromJson(value.toString(), Customer.class);
        context.write(new Text("key"), new Text("value"));
    }
}