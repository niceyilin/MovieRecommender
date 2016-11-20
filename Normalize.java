
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * Created by yilin on 11/19/16.
 */
public class Normalize {

    public static class NormalizeMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String toPlusFrom =  value.toString().trim().split("\\s+")[0];
            String toMovie = toPlusFrom.split("<-")[0];
            String fromMovie = toPlusFrom.split("<-")[1];
            String count = value.toString().trim().split("\\s+")[1];

            context.write(new Text(toMovie), new Text(fromMovie + "=" + count));
        }

    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, DoubleWritable>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0f;
            ArrayList<String> copies = new ArrayList<String>();
            String toMovie = key.toString();

            for(Text value : values){
                double count = Double.parseDouble(value.toString().trim().split("=")[1]);
                sum += count;
                copies.add(value.toString()); // CAUTION : adding "Text" to list won't work !
            }

            for(String value : copies){  // CAUTION : have to read though cache "copies", iterate values twice won't work !
                String fromMovie = value.trim().split("=")[0];
                double count = Double.parseDouble(value.trim().split("=")[1]);
                double normalizedCount = count / sum;

                context.write(new Text(toMovie + "<-" + fromMovie), new DoubleWritable(normalizedCount));
            }

        }

    }


    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Normalize.class);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.waitForCompletion(true);
    }

}
