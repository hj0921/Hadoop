import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text Value, Context context) throws IOException, InterruptedException{
            /* example:
            input Value: 1 \t 2,7,8,29  can be read as --> from 1 to 2,7,8,29
            output context -->
            1 \t 2=1/4,
            1 \t 7=1/4,
            1 \t 8=1/4,
            1 \t 29=1/4
            */
            String line = Value.toString().trim();
            String[] fromTo = line.split("\t");

            // check if it is a dead end
            if(fromTo.length == 1 || fromTo[1].trim().equals("")) {
                return;
            }

            // separate the value to get the from and tos, and write the relation to the context
            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to: tos){
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            /*
            input value: id \t probability
            example: 1 \t 1/6012
             */
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text>{

        float beta;

        @Override
        /*
        Set up beta, avoid dead end or spider traps
        simulating what will a user do if user meet the dead end or the spider trap.
        User has beta probability just close the web and go to other web
         */
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            /*
            input key: fromID  --> 1
            input value: <toid1=prob1, toid2=prob2, ..., sub pr0> --> <2=1/4, 7=1/4,8=1/4, 29=1/4, 1/6012>

            output key: toID --> 2
            output value: subPr --> 1/4 * 1/6012 * (1-beta)
            ...
             */
            List<String> transitionUnit = new ArrayList<String>();
            double prUnit = 0;
            for (Text value: values){
                //check whether the value comes from the TransitionMapper or the PRMapper
                if (value.toString().contains("=")){
                    // value from TransitionMapper
                    transitionUnit.add(value.toString());
                }else{
                    //value from PRMapper, it is a sub pr value
                    prUnit = Double.parseDouble(value.toString());
                }
            }
            // go through all the to=prob combinations
            for (String unit: transitionUnit) {
                String outputKey = unit.split("=")[0];
                double relation = Double.parseDouble(unit.split("=")[1]);
                String outputValue = String.valueOf(relation * prUnit * (1 - beta));
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
        }
    }

