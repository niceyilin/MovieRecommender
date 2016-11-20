import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilin on 11/17/16.
 */
public class CoOccurance {

    public  static class CoOccuranceMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<Integer> movieList = new ArrayList<Integer>();

            // value is not the key-value, it's a whole line of input
            String lefthalf = value.toString().trim().split("\\s+")[1];
            String [] arr = lefthalf.split(",");

            for(String s : arr){
                int movieid = Integer.parseInt(s.split("=")[0]);
                movieList.add(movieid);
            }

            for(Integer toMovie : movieList){
                for(Integer fromMovie : movieList){
                    context.write(new Text(toMovie + "<-" + fromMovie), new IntWritable(1));
                }
            }
        }

    }

    public static class CoOccuranceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
                //context.write(key, value);
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String [] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CoOccurance.class);
        job.setMapperClass(CoOccuranceMapper.class);
        job.setReducerClass(CoOccuranceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
