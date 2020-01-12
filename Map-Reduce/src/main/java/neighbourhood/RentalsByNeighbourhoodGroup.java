package neighbourhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.lang.StringUtils;
import java.io.IOException;

    public class RentalsByNeighbourhoodGroup {

        public static class RentalsGroupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

            // define IntWritable variable as value
            private final static IntWritable one = new IntWritable(1);
            // define Text variable to store as key
            Text cntText = new Text();
            // define temporary error line
            String store = "";

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
                        cntText.set(keyvalue[4].trim());
                        context.write(cntText, one);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public static class RentalsGroupReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
                System.err.println("Usage: WordCount <in> <out>");
                System.exit(2);
            }

            // hadoop classes
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(RentalsByNeighbourhoodGroup.class);
            job.setMapperClass(RentalsGroupMapper.class);
            job.setCombinerClass(RentalsGroupReducer.class);
            job.setReducerClass(RentalsGroupReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
