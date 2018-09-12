import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class FriendsReducer extends
        Reducer<Text, Text, Text, Text> {

    //Input: <Text key, Iterable<Text> value>
    //Output: <Text key, Text Value>

    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //Initialize the result string to build the common friends list
        String result = "";

        //Set a counter to differentiate between friend list 1 and 2
        // only two friends are mapped together, so it can only be friend list 1 and 2
        int count = 0;

        //Initialize List<String> containers to store the friends list of each friends list
        List<String> first = new ArrayList<String>(Arrays.asList(""));
        List<String> second = new ArrayList<String>(Arrays.asList(""));

        //Set the List<String> friends lists using the comma delimiter
        for (Text str : values){
            if (count == 0){
                first = new ArrayList<String>(Arrays.asList(str.toString().split(",")));
                count++;
            }else{
                second = new ArrayList<String>(Arrays.asList(str.toString().split(",")));
            }
        }

        //Check the make sure the lists have at least one entry
        if (first.size() > 0 && second.size() > 0){
            //Iterate through the lists to identify common friends and concatenate them to the result string
            for (int i = 0; i < first.size(); i++) {
                for(int j = 0; j < second.size(); j++){
                    if (first.get(i).compareToIgnoreCase(second.get(j)) == 0 ) {
                        result = result + first.get(i) + " ";
                    }
                }
            }
        }

        //Set up the output variable
        Text output = new Text();
        //Add in parentheses, arrow, and remove the trailing space in the result string to be used as the output
        output.set("(" + text + ")->(" + result.substring(0,result.length()-1) + ")");

        //Write the <key,value> pair to be output
        // they key is null, using only output to communicate formatted information
        context.write(null, output);

    }
}