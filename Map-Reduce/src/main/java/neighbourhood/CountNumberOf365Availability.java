package neighbourhood;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;

public class CountNumberOf365Availability {

    public static class Count365AvailabilityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable sam = new IntWritable(1);
        Text cntText = new Text();
        Text availability = new Text();
        String store = "";
        int rec = 0;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                if (value.toString().contains("neighbourhood_group"))
                    return;
                else {

                    String[] keyvalue = new String[0];
                    int count = StringUtils.countMatches(value.toString(),"\"");

                    if((count%2)!= 0 && store == "") {
                        store = value.toString();
                        System.out.println(store);
                        return;
                    } else if((count%2)!= 0 && store != "") {
                        String comb = store + value.toString();
                        keyvalue = comb.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                        System.out.println(comb);
                        store = "";
                    } else {
                        keyvalue = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                    }

                    System.out.println(keyvalue[keyvalue.length-1]);
                    System.out.println(keyvalue[0]);

                    if(Integer.parseInt(keyvalue[keyvalue.length-1]) == 365){
                        availability.set(keyvalue[keyvalue.length-1]);
                        context.write(availability, one);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Count365AvailabilityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Error count");
            System.exit(2);
        }

        // hadoop classes
        Job job = Job.getInstance(conf, "count");
        job.setJarByClass(CountNumberOf365Availability.class);
        job.setMapperClass(Count365AvailabilityMapper.class);
        job.setCombinerClass(Count365AvailabilityReducer.class);
        job.setReducerClass(Count365AvailabilityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
