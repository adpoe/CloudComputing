// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MeanMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        /** Old format
        //String year = line.substring(15, 19);
        String station = line.substring(0,11);
        //String stationDate = line.substring(0,17);
        String date = line.substring(12,17);
        //String stationDate = station + '\t' + date;
        */

        // get key from new format
        //String stationDate = line.substring(0,20);
        String station = line.substring(0,12);
        String dayMonth =  line.substring(16,20);
        String stationDayMonth = station + dayMonth;
        int airTemperature;
        // old way --> airTemperature = Integer.parseInt(line.substring(18,line.length()));
        airTemperature = Integer.parseInt(line.substring(21,line.length()));

        // old way --> context.write(new Text(date), new IntWritable(airTemperature));
        context.write(new Text(stationDayMonth), new IntWritable(airTemperature));
    }
}