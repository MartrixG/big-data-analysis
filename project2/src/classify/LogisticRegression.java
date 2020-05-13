package classify;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import utils.FileOperator;

import java.io.IOException;

public class LogisticRegression {
	public static boolean run(String globalHdfsPath, String thetaPath, String trainDataPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		conf.setDouble("lr", 0.05);
		conf.set("theta", thetaPath);
		Job job = Job.getInstance(conf, "logistic");
		job.setJarByClass(LogisticRegression.class);

		// Map设置
		job.setMapperClass(LogisticMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		// Reduce设置
		job.setReducerClass(LogisticReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(job, new Path(trainDataPath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(outPath));

		return job.waitForCompletion(true);
	}

	static class LogisticMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		public double lr = 0.0f;
		public Double[] Xi = null;
		public Double[] theta = null;

		@Override
		// 初始化逻辑回归所需参数
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			lr = conf.getDouble("lr", 0.0f);
			Xi = new Double[20];
			theta = new Double[20];
			String[] thetaData = FileOperator.getContentFromHDFS(conf.get("fs.defaultFS"), conf.get("theta")).split("\n");
			for(int i=0;i<20;i++){
				theta[i] = Double.parseDouble(thetaData[i].split(" ")[1]);//从文件加载所有20个参数
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] temp = value.toString().split(",");
			for (int i = 0; i < 20; i++) {
				Xi[i] = Double.parseDouble(temp[i]);//加载一条数据
			}
			double exp = 0;
			for (int i = 0; i < 20; i++) {
				exp += (Xi[i] * theta[i]);
			}
			double predict = (1 / (1 + (Math.exp(-exp))));// 计算似然函数
			double Yi = Double.parseDouble(temp[temp.length - 1]);
			for (int i = 0; i < 20; i++) {
				double update = theta[i] + lr * (Yi - predict) * (Xi[i]); // 计算梯度并且更新参数
				theta[i] = update;
			}
			for (int i = 0; i < 20; i++) {
				context.write(new Text(i + " "), new DoubleWritable(theta[i]));// 按照顺序将参数传递给reducer
			}
		}
	}

	static class LogisticReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			float sum = 0;
			int count = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum / count));//重新将参数写入到文件，方便下一轮迭代
		}
	}
}
