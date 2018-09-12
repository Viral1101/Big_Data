import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ResponseReducer extends
        Reducer<Text, FloatWritable, Text, FloatWritable> {

    //Input: <Text key, Iterable<FloatWritable> value>
    //Output: <Text key, FloatWritable value>

    public void reduce(Text text, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        //Maintain a count in order to take the average of the response times
        int count = 0;

        //Initialize the response variable to contain the sum of the response times
        float response = 0.0f;

        //Iterate through each response time
        for (FloatWritable val:values){
            //Skip over any value encoded as 0 since it is missing data
            if (!val.equals(0)){
                //Sum up the response times and increase the count
                response += val.get();
                count ++;
            }

        }

        //Calculate the average response time
        FloatWritable average = new FloatWritable(response/count);

        //Write the <key,value> as output
        context.write(text, average);

    }
}