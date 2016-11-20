import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by yilin on 11/19/16.
 */
public class Sum {

    public static class SumMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String userPlusMovie = value.toString().trim().split("\\s+")[0];
            double score = Double.parseDouble(value.toString().trim().split("\\s+")[1]);

            context.write(new Text(userPlusMovie), new DoubleWritable(score));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0f;

            for (DoubleWritable value : values){
                sum += value.get();
            }

            DecimalFormat df = new DecimalFormat("#.000");
            sum = Double.parseDouble(df.format(sum));

            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String [] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Sum.class);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
