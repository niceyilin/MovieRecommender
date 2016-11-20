import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilin on 11/19/16.
 */
public class Multiplication {

    public static class ColomnizeMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String toPlusFrom = value.toString().trim().split("\\s+")[0];
            String count = value.toString().trim().split("\\s+")[1];
            String toMovie = toPlusFrom.split("<-")[0];
            String fromMovie = toPlusFrom.split("<-")[1];

            context.write(new Text(fromMovie), new Text(toMovie + "=" + count));
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String userid = value.toString().trim().split(",")[0];
            String movieid = value.toString().trim().split(",")[1];
            String rating = value.toString().trim().split(",")[2];

            context.write(new Text(movieid), new Text(userid + ":" + rating));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movieList = new ArrayList<String>();
            List<String> userList = new ArrayList<String>();

            for (Text value : values){
                String s = value.toString();
                if (s.contains("=")){
                    movieList.add(s);
                }
                else if (s.contains(":")){
                    userList.add(s);
                }
            }

            for (String user : userList){
                String userid = user.split(":")[0];
                double rating = Double.parseDouble(user.split(":")[1]);

                for (String movie : movieList){
                    String movieid = movie.split("=")[0];
                    double cooccurance = Double.parseDouble(movie.split("=")[1]);

                    String mp = String.valueOf(rating * cooccurance);

                    context.write(new Text(userid + ":" + movieid), new Text(mp));
                }
            }

            //for (Text value : values){
            //    context.write(key, value);
            //}
        }
    }

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Multiplication.class);
        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ColomnizeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}
