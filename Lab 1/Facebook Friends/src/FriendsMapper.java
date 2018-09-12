import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendsMapper extends
        Mapper<Object, Text, Text, Text> {

    //Input: <Object key, Text value>
    //Output: <Text key, text value>

    private Text person = new Text();
    private Text friendsList = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        //File input: Line 1 >> A->B,C,D

        //Split the input into two Strings using the arrow as the delimiter
        String[] input = value.toString().split("->");

        //Store the full friends list to write to output
        friendsList.set(input[1]);

        //Split the friends list using the comma delimiter into a String array
        String[] friends = input[1].split(",");

        //Iterate over each item in friends to connect the person to each of their friends
        // A,B
        // A,C
        // A,D
        for (String str : friends) {

            //Order the person/friend combination alphabetically so that keys will match during the Shuffle operation
            if(input[0].compareToIgnoreCase(str) < 0){
                person.set(input[0] + "," + str);
            }else{
                person.set(str + "," + input[0]);
            }

            //Write the <key,value> output
            context.write(person, friendsList);
        }
    }
}
