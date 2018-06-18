import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.*;
import java.io.*;
import java.net.*;


public class WordCount {

    //嵌套类 Mapper
    //Mapper<keyin,valuein,keyout,valueout>
    public static class SingleClassMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        //map(WritableComparable, Writable, Context)
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
            while (tokenizer.hasMoreTokens()) {
                String line = tokenizer.nextToken();
                StringTokenizer tk = new StringTokenizer(line.toString(), " ");
                //获取学号
                String idString = tk.nextToken().toString();
                //获取班级
                String student_class = idString.substring(8, 10);
                //获取名字
                String nameString = tk.nextToken().toString();
                //获取成绩
                String scoreString = tk.nextToken().toString();

                /*
                System.out.println("/////////////////////////////////");
                System.out.println(line);
                System.out.println("/////////////////////////////////");
                */
                context.write(new Text(student_class), new Text(line));
            }
        }
    }

    //嵌套类Reducer
    //Reduce<keyin,valuein,keyout,valueout>
    //Reducer的value in类型要和Mapper的value out类型一致,Reducer的valuein是Mapper的valueout经过shuffle之后的值
    public static class SingleClassReducer extends Reducer<Text, Text, Text, Text> {
        //private IntWritable result = new IntWritable();

        @Override

        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            int min = 101, max = -1, score, sum = 0;
            long avg = 0;
            String idStr = "", nameStr = "", scoreStr = "";
            String[] infoStr;

            String maxInfo = "";
            String minInfo = "";
            String valueStr = "";
            Text result = new Text();
            int count = 0;
            //Iterator<Text> ite = values.iterator();
            //while(ite.hasNext())
            for (Text val : values) {
                System.out.println(val);
                StringTokenizer tokenizer = new StringTokenizer(val.toString(), " ");
                idStr = tokenizer.nextToken();
                nameStr = tokenizer.nextToken();
                scoreStr = tokenizer.nextToken();
                score = Integer.parseInt(scoreStr.toString());

                if (score > max) {
                    max = score;
                    maxInfo = String.valueOf(idStr) + " " + String.valueOf(nameStr);
                    //System.out.println(maxInfo);
                } else if (score == max) {
                    maxInfo = maxInfo + "|" + String.valueOf(idStr) + " " + String.valueOf(nameStr);
                }
                if (score < min) {
                    min = score;
                    minInfo = String.valueOf(idStr) + " " + String.valueOf(nameStr);
                } else if (score == min) {
                    minInfo = minInfo + "|" + String.valueOf(idStr) + " " + String.valueOf(nameStr);
                }
                count++;
                sum += score;
            }
            avg = sum / count;
            valueStr = "class" + key.toString() +
                    ",max:" + maxInfo + ":" + String.valueOf(max) +
                    ",min:" + minInfo + ":" + String.valueOf(min) +
                    ",avg:" + String.valueOf(avg);
            //class19                                            ,
            // max : 201501061911 QWQ | 201501061922 DS :  65    ,
            // min : 201501061911 QG  | 201501061922 qxx : 10   ,
            // avg :  10
            result.set(valueStr);
            context.write(key, result);
        }
    }
    //class01;max:|2015 www 99;min:|2015-we.20);
    public static class MultiClassMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        //map(WritableComparable, Writable, Context)
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(new Text("ClassInfo"), new Text(value));
            }
    }


    //嵌套类Reducer
    //Reduce<keyin,valuein,keyout,valueout>
    //Reducer的value in类型要和Mapper的value out类型一致,Reducer的valuein是Mapper的valueout经过shuffle之后的值
    public static class MultiClassReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            int max_score = 0, min_score = 101, count = 0, score_max_tmp = 0, score_min_tmp = 0;
            int h_score, l_score;
            long avg_sum = 0;
            String max_info = "", min_info = "", class_num;
            String max_final = "", min_final = "";
            String[] info;
            //class09,
            // max:201501060909 HSX:95,
            // min:201501060905 GXV:2,
            // ;avg:53
            for (Text val : values) {
                info = String.valueOf(val).split(",");
                class_num = info[0];
                max_info = info[1].split(":")[1];
                h_score = Integer.parseInt(info[1].split(":")[2]);
                min_info = info[2].split(":")[1];
                l_score = Integer.parseInt(info[2].split(":")[2]);
                avg_sum += Long.parseLong(info[3].split(":")[1]);

                if (h_score > max_score) {
                    max_score = h_score;
                    max_final = max_info;
                } else if (h_score == max_score) {
                    max_final = max_final + " | " + max_info;
                }

                if (l_score < min_score) {
                    min_score = l_score;
                    min_final = min_info;
                } else if (l_score == min_score) {
                    min_final = min_final + " | " + min_info;
                }
                count++;
            }
            avg_sum = avg_sum / count;
            String final_info = "\nmax info : " + max_final + " , and their score is : " + String.valueOf(max_score) + "\n"
                    + "min info : " + min_final + " , and their score is : " + String.valueOf(min_score) + "\n"
                    + "avg score is : " +  String.valueOf(avg_sum);
            context.write(new Text("Statistics:\n"), new Text(final_info));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();//获得Configuration配置 Configuration: core-default.xml, core-site.xml
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {//判断输入参数个数，不为两个异常退出
            System.err.println("please input <input path> <output path>");
            System.exit(2);
        }

        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration(), "root");
        //Path src = new Path(otherArgs[2])
        Path dst = new Path(otherArgs[0]);//input path
        File file = new File(otherArgs[2]);
        LinkedList<File> list = new LinkedList<File>();
        File[] files = file.listFiles();
        for (File file2 : files) {
            System.out.println(file2.getAbsolutePath());
            Path src = new Path(file2.getAbsolutePath());
            fs.copyFromLocalFile(src, dst);
        }
        fs.close();
        /////////////////////////////////////////////////////////////////////////////
        ////设置Job属性
        Job job = new Job(conf, "job1");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(SingleClassMapper.class);
        //job.setCombinerClass(SingleClassReducer.class);//将结果进行局部合并
        job.setReducerClass(SingleClassReducer.class);

        //指定mapper输出数据kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //指定最终输出数据的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//传入input path
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//传入output path，输出路径应该为空
        //System.exit(job.waitForCompletion(true)?0:1);//是否正常退出

        ControlledJob ctrl_job1 = new ControlledJob(conf);
        ctrl_job1.setJob(job);
        /////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////
        Job job2 = new Job(conf, "job2");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(MultiClassMapper.class);
        job2.setReducerClass(MultiClassReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));//传入input path
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + '2'));//传入output path，输出路径应该为空，否则报错org.apache.hadoop.mapred.FileAlreadyExistsException。

        //加入控制容器
        ControlledJob ctrl_job2 = new ControlledJob(conf);
        ctrl_job2.setJob(job2);

        //设置多个作业直接的依赖关系
        ctrl_job2.addDependingJob(ctrl_job1);
        //   System.exit(job2.waitForCompletion(true)?0:1);//是否正常退出
        //////////////////////////////////////////////////////////////////////////////////

        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("myctrl");
        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrl_job1);
        jobCtrl.addJob(ctrl_job2);

        Thread t = new Thread(jobCtrl);
        t.start();
        while (true) {
            if (jobCtrl.allFinished()) {//如果作业成功完成，就打印成功作业的信息
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
    }
}


