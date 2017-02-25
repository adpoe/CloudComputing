// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int length = 0;
        int sumValue = 0;
        for (IntWritable value : values) {
            sumValue += value.get();
            length += 1;
        }
        int meanValue = sumValue / length;
        context.write(key, new IntWritable(meanValue));
    }
}
// ^^ MaxTemperatureReducer
