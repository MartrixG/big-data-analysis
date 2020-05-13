package classify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class NaiveBayes {
	public static boolean run(String globalHdfsPath, String trainDataPath, String formalParamPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		Job job = Job.getInstance(conf, "Bayes");
		job.setJarByClass(NaiveBayes.class);

		// Map设置
		job.setMapperClass(BayesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce设置
		job.setReducerClass(BayesReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		//输出文件设置
		FileInputFormat.setInputPaths(job, new Path(trainDataPath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(formalParamPath));

		return job.waitForCompletion(true);
	}

	static class BayesMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String point = value.toString();
			Text classType = new Text(point.substring(point.length() - 1));// 获得所属类型
			Text data = new Text(point.substring(0, point.length() - 2));// 获得数据内容
			context.write(classType, data);// 传递给reducer
		}
	}

	static class BayesReducer extends Reducer<Text, Text, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			// 初始化均值
			double[] mean = new double[20];
			double[] D = new double[20];
			// 保存所有的此类型的数据，由于迭代器只可以迭代一次，所以需要保存数据
			ArrayList<ArrayList<Double>> points = new ArrayList<>();
			for (Text value : values) {
				count++;//数据统计量加一
				String[] tmpPointS = value.toString().split(",");
				ArrayList<Double> tmpPointD = new ArrayList<>();
				for (int i = 0; i < 20; i++) {
					double tmpCoordinate = Double.parseDouble(tmpPointS[i]);// 计算当前点的第i个属性的数值
					tmpPointD.add(tmpCoordinate);
					mean[i] += tmpCoordinate;// 叠加到均值上
				}
				points.add(tmpPointD);
			}
			// 计算均值
			for (int i = 0; i < 20; i++) {
				mean[i] /= count;
			}
			// 计算方差
			for (ArrayList<Double> point : points) {
				for (int i = 0; i < 20; i++) {
					D[i] += (point.get(i) - mean[i]) * (point.get(i) - mean[i]);
				}
			}
			for (int i = 0; i < 20; i++) {
				D[i] /= count;
			}
			//构造输出，总共四十个参数（均值、方差）
			StringBuilder re = new StringBuilder();
			for (int i = 0; i < 20; i++) {
				re.append(mean[i]).append(",");
			}
			for (int i = 0; i < 20; i++) {
				re.append(D[i]).append(",");
			}
			//先验
			re.append((double) count/ 900000.0 + ",");
			re.append(key.toString());
			context.write(NullWritable.get(), new Text(re.toString()));
		}
	}

}
