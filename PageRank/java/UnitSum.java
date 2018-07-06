import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            /*
            input value: id \t sub pr
            output key: id
            output value: sub pr
             */
            String[] pageSubrank = value.toString().split("\t");
            double subPR = Double.parseDouble(pageSubrank[1]);
            context.write(new Text(pageSubrank[0]), new DoubleWritable(subPR));
        }
    }

    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        float beta;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] pr = value.toString().trim().split("\t");
            double pr_beta = Double.parseDouble(pr[1]) * beta;
            context.write(new Text(pr[0]), new DoubleWritable(pr_beta));
        }

    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            /*
            input example:
            key = 2, value = 1/4*subpr1*(1-beta)
            key = 2, value = 1/3*subpr2*(1-beta)
            key = 2, value = subpr3*beta
            ...

            output example:
            key = 2, value = 1/4*subpr1*(1-beta) + 1/3*subpr2*(1-beta) + subpr3*beta + ...
             */
            double sum = 0;
            for (DoubleWritable value: values){
                sum += value.get();
            }
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

        ChainMapper.addMapper(job, PassMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        ChainMapper.addMapper(job, BetaMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
