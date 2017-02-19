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
        //String year = line.substring(15, 19);
        String station = line.substring(0,11);
        //String stationDate = line.substring(0,17);
        String date = line.substring(12,17);
        //String stationDate = station + '\t' + date;

        int airTemperature;
        airTemperature = Integer.parseInt(line.substring(18,line.length()));
        context.write(new Text(date), new IntWritable(airTemperature));
    }
}