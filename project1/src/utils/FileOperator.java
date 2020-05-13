package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 使用JAVA API和hdfs进行文件交互
 */
public class FileOperator {
	/**
	 * 检测hdfs是否存在文件
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param path           查询的文件路径
	 * @return 如果存在true, 否则false
	 * @throws IOException
	 */
	public static boolean testExit(String globalHdfsPath, String path) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		return fs.exists(new Path(path));
	}

	/**
	 * 删除目录
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param hdfsDir        目录路径
	 * @return 删除成功返回true, 否则返回false
	 * @throws IOException
	 */
	public static boolean rmdir(String globalHdfsPath, String hdfsDir) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(hdfsDir);
		boolean result = fs.delete(hdfsPath, true);
		fs.close();
		return result;
	}

	/**
	 * 复制本地文件到hdfs
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param localPath      本地文件路径
	 * @param hdfsPath       hdfs文件路径
	 * @throws IOException
	 */
	public static void putFileToHDFS(String globalHdfsPath, String localPath, String hdfsPath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path localFile = new Path(localPath);
		Path hdfsFile = new Path(hdfsPath);
		fs.copyFromLocalFile(false, true, localFile, hdfsFile);
	}

	/**
	 * 从hdfs读取文件全部内容
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param hdfsPath       hdfs文件路径
	 * @return String形式的文件全部内容
	 * @throws IOException
	 */
	public static String getContentFromHDFS(String globalHdfsPath, String hdfsPath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(hdfsPath);
		if (fs.exists(file)) {
			FSDataInputStream inStream = fs.open(file);
			String re = new String(inStream.readAllBytes());
			return re;
		}
		return null;
	}
}