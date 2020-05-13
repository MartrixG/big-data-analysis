package operator;

import evaluation.Evaluation;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.FileOperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Filter {
	public static boolean run(String globalHdfsPath, String InputPath, String OutputPath, String ratingPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		// 使用Hadoop的configuration功能, 将最值和均值传递给所有Reducer
		String[] maxMin = FileOperator.getContentFromHDFS(globalHdfsPath, ratingPath + "/part-r-00000").split("\n");
		conf.set("ratingMax", maxMin[0]);
		conf.set("ratingMin", maxMin[1]);
		conf.set("ratingAvg", maxMin[2]);
		Job job = Job.getInstance(conf, "Filter");
		job.setJarByClass(Filter.class);
		// Map设置
		job.setMapperClass(FilterMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Evaluation.class);

		// Reduce设置
		job.setReducerClass(FilterReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 输出文件设置
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(InputPath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(OutputPath));

		return job.waitForCompletion(true);
	}

	static class FilterMap extends Mapper<LongWritable, Text, Text, Evaluation> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Evaluation tmp = new Evaluation(value.toString());//直接将输入字符串转换成对象
			//检测经纬度是否为奇异值, 只保留合理的值
			if(8.1461259 <= tmp.longitude && tmp.longitude <= 11.1993265 && 56.5824856 <= tmp.latitude && tmp.latitude <= 57.750511){
				context.write(new Text(tmp.user_career), tmp);
			}
		}
	}
	static class FilterReduce extends Reducer<Text, Evaluation, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Evaluation> values, Context context) throws IOException, InterruptedException {
			List<Evaluation> pool = new ArrayList<>();
			Configuration conf = context.getConfiguration();
			// 利用configuration获取到rating的最值和均值
			Double maxRating = Double.valueOf(conf.get("ratingMax")),
				   minRating = Double.valueOf(conf.get("ratingMin")),
				   avgRating = Double.valueOf(conf.get("ratingAvg"));
			// 注释部分是只是用两轮MapReduce所额外执行的语句
			// Double minRating = Double.POSITIVE_INFINITY, maxRating = Double.NEGATIVE_INFINITY, ratingSum = 0.0, tot = 0.0;
			Map<String, Double> nationCareerIncome = new HashMap<>();
			Map<String, Integer> nationCareerCount = new HashMap<>();
			for(Evaluation value : values) {
				pool.add(value.clone());
				// 注释部分是只是用两轮MapReduce所额外执行的语句
				/*
				if(value.rating <= 100.0 && value.rating >= 0.0) {
					minRating = Double.min(minRating, value.rating);
					maxRating = Double.max(maxRating, value.rating);
					tot++;
					ratingSum += value.rating;
				}
				 */
				// 若不是空值, 则对当前职业的国家平均工资进行更新
				if(value.user_income!=-1.0){
					double preTot = nationCareerIncome.get(value.user_nationality)==null ? 0.0
									: nationCareerIncome.get(value.user_nationality);
					nationCareerIncome.put(value.user_nationality,value.user_income + preTot);// 更新总工资数, 为了计算平均工资
					int count= preTot == 0.0 ? 0 : nationCareerCount.get(value.user_nationality);
					nationCareerCount.put(value.user_nationality, count + 1);
				}
			}
			// avgRating = ratingSum / tot;
			for(Evaluation eachEVA : pool) {
				// 对收入的空值进行补充
				if(eachEVA.user_income == -1.0){
					eachEVA.user_income = nationCareerIncome.get(eachEVA.user_nationality) / nationCareerCount.get(eachEVA.user_nationality);
				}
				// 对离群或者空值进行修改
				if(eachEVA.rating < 0 || eachEVA.rating > 100){
					eachEVA.rating = avgRating;
				}
				// 计算归一化后的rating
				eachEVA.rating = (eachEVA.rating - minRating) / (maxRating - minRating);
			}
			for(Evaluation each : pool){
				context.write(NullWritable.get(), new Text(each.toString()));
			}
		}
	}
}
