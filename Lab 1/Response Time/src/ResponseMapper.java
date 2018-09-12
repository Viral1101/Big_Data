import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ResponseMapper extends
        Mapper<Object, Text, Text, FloatWritable> {

    //Input: <Object key, Text value>
    //Output: <Text key, FloatWritable value>

    //declare variables
        //Store key value as Region #: Precinct
    private Text locale = new Text();
        //Store response time
    private FloatWritable vals = new FloatWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        //CSV format includes the region in column 2, precinct in column 3
        // and the response time in column 4
        // Note: response times were converted in the data to be numeric in hours,
        //  missing data was overwritten as 0.

        //Split the CSV line by the comma delimiter into a String array
        String[] input = value.toString().split(",");

        //Set the location to a combination of Region # and precinct
        locale.set(input[2] + ": " + input[3]);

        //Set the storage variable for the response time
        vals.set(Float.parseFloat(input[4]));

        //Write out the new <key,value> pair
        context.write(locale, vals);

    }
}
