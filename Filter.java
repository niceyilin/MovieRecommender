
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.*;

/**
 * Created by yilin on 11/19/16.
 */
public class Filter {

    public static class RecommendListMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String userPlusMovie = value.toString().trim().split("\\s+")[0];
            String score = value.toString().trim().split("\\s+")[1];
            String user = userPlusMovie.split(":")[0];
            String movie = userPlusMovie.split(":")[1];

            context.write(new Text(user), new Text(movie + ":" + score));
        }

    }

    public static class WatchedMovieMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String user = value.toString().trim().split("\\s+")[0];
            String movies = value.toString().trim().split("\\s+")[1];
            String [] moviePlusRatingList = movies.split(",");

            for(String moviePlusRating : moviePlusRatingList){
                context.write(new Text(user), new Text(moviePlusRating));
            }
        }

    }

    private static class Movie{
        private String id;
        private double score;
        Movie(String id, double score){
            this.id = id;
            this.score = score;
        }
    }

    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {

        private int topK;

        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            this.topK = conf.getInt("topK", 2);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            HashSet<String> recommendSet = new HashSet<String>();
            HashSet<String> watchedMovieSet = new HashSet<String>();
            StringBuilder recommendString = new StringBuilder();

            PriorityQueue<Movie> minHeap = new PriorityQueue<Movie>(topK, new Comparator<Movie>() {
                public int compare(Movie m1, Movie m2) {
                    if( m1.score > m2.score)  return 1;
                    return 0;
                }
            });

            while(values.iterator().hasNext()){
                String s = values.iterator().next().toString().trim();
                if (s.contains("=")){
                    String movieid = s.split("=")[0];
                    watchedMovieSet.add(movieid);
                }
                else if (s.contains(":")){
                    recommendSet.add(s);
                }
            }

            for (String reccommendMovie : recommendSet){
                String movieid = reccommendMovie.split(":")[0];
                double score = Double.parseDouble(reccommendMovie.split(":")[1]);

                if(!watchedMovieSet.contains(movieid)){
                    // to get topK out of recommend list
                    minHeap.add(new Movie(movieid, score));
                    if (minHeap.size() > topK){
                        minHeap.poll();
                    }
                }
            }

            while (minHeap.size() > 0){
                Movie m = minHeap.poll();
                recommendString.append(m.id);
                recommendString.append(" (score ");
                recommendString.append(m.score);
                recommendString.append(")\t");
            }

            String outputKey = "Recommended movies for User " + key.toString() + " :\t";
            context.write(new Text(outputKey), new Text(recommendString.toString()));
        }

    }


    public static void main(String [] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        conf.setInt("topK", 1);

        job.setJarByClass(Filter.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RecommendListMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, WatchedMovieMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

    }
}
