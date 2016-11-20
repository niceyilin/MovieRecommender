import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by yilin on 11/16/16.
 */
public class GroupByUser {

    enum GroupByUserCounters{
        MapperError,
        movieListCounter,
        ReducerError,
        ReduceCounter
    }


    public static class GroupByUserMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String [] inputArray = value.toString().trim().split(",");

            if(inputArray.length != 3){ // user id, movie id ,rating
                context.getCounter(GroupByUserCounters.MapperError).increment(1);
            }

            String userId = inputArray[0];
            String movieId = inputArray[1];
            String rating = inputArray[2];

            context.write(new Text(userId), new Text(movieId + "=" + rating));
        }

    }

    public static class GroupByUserReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String outputVal = new String();

            for(Text value :values){
                outputVal += value.toString();
                outputVal += ",";
            }

            if(outputVal.length() > 0 && outputVal.charAt(outputVal.length()-1) == ','){
                outputVal = outputVal.substring(0, outputVal.length()-1);
            }

            context.write(key, new Text(outputVal));
        }

    }


    public static void main(String [] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(GroupByUser.class);
        job.setMapperClass(GroupByUserMapper.class);
        job.setReducerClass(GroupByUserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
