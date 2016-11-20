import java.io.*;
import java.util.*;

/**
 * Created by yilin on 11/17/16.
 */
public class PreProcess {

    private String inputFile, outputFile;

    public PreProcess(String inputFile, String outputFile){

        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    public void addAllMovies() throws IOException {

        HashSet<String> allMovies = new HashSet<String>();
        HashMap<String, HashMap<String, String>> watchedMovies = new HashMap<String, HashMap<String, String>>();

        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        String line = br.readLine();

        while(line != null){
            String [] inputArray = line.trim().split(",");
            if(inputArray.length != 3){
                System.out.println("Error : input are not 'user, movie, rating' ! ");
                return;
            }
            String userid = inputArray[0];
            String movieid = inputArray[1];
            String rating = inputArray[2];

            // keep all the unique movies
            allMovies.add(movieid);

            // keep all watched movies for each user
            if(!watchedMovies.containsKey(userid)){
                HashMap<String, String> newRecord = new HashMap<String, String>();
                newRecord.put(movieid, rating);
                watchedMovies.put(userid, newRecord);
            }
            else{
                watchedMovies.get(userid).put(movieid, rating);
            }

            line = br.readLine();
        }

        PrintWriter output = new PrintWriter(outputFile, "UTF-8");

        //  check each user's watched list
        for(Map.Entry<String, HashMap<String, String>> entry : watchedMovies.entrySet()){
            String userid = entry.getKey();
            HashMap<String, String> movierating = entry.getValue();

            // calculate average rating for unwatched movies
            float sum = 0f, averageRating = 0f;
            int count = 0;
            for(Map.Entry<String, String> entry2 : movierating.entrySet()){
                sum += Float.parseFloat(entry2.getValue());
                count++;
            }
            averageRating = sum / count;

            // find which movies hasn't been watched by this user
            for(String movieid : allMovies){
                if(!movierating.containsKey(movieid)){
                    movierating.put(movieid, String.valueOf(averageRating));
                }
            }

            // write to output file
            /*    this already does "GroupByUser"
            StringBuilder sb = new StringBuilder();

            sb.append(userid+"\t");

            for(Map.Entry<String, String> entry3 : movierating.entrySet()){
                sb.append(entry3.getKey() + "=" + entry3.getValue());
                sb.append(",");
            }

            String s = sb.toString();

            if(s.charAt(s.length()-1) == ','){
                s = s.substring(0, s.length()-1);
            }
            */

            for(Map.Entry<String, String> entry3 : movierating.entrySet()){
                output.print(userid + "," + entry3.getKey() + "," + entry3.getValue() + "\n");
            }

        }

        output.close();

    }

    public static void main(String [] args) throws IOException {
        PreProcess PP = new PreProcess(args[0], args[1]);
        PP.addAllMovies();
    }

}
