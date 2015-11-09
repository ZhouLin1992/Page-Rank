import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	private static String N_sum = "";
	private static String initialized = "false";
	private static long oldConvergeCount = -1;


	public static class CalNMapper
	extends Mapper<Object,// input key
	Text,  // input value
	Text,  // output key
	IntWritable> //output value
	{
		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write (new Text("a"),one);

		}
	}

	public static class CalNReducer
	extends Reducer<Text,IntWritable,IntWritable,NullWritable> {
		private IntWritable N_result = new IntWritable();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int N_sum = 0;
			for (IntWritable val : values){
				N_sum += val.get();
			}
			N_result.set(N_sum);
			context.write(N_result,null);
		}

	}


	public static class CalEdgesMapper
	extends Mapper<Object,// input key
	Text,  // input value
	Text,  // output key
	IntWritable> //output value
	{
//		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] value_part = value.toString().split(" ");
//			context.write(new Text("a"), one);
			context.write(new Text("b"), new IntWritable(value_part.length -1));

		}
	}

	public static class CalEdgesReducer
	extends Reducer<Text,IntWritable,Text,Text> {
		private Text edgesNum_result = new Text();
		private Text max_result = new Text();
		private Text min_result = new Text();
		private Text average_result = new Text();
	//	private IntWritable N_result = new IntWritable();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {

			int edgesNum = 0;
			int outDegree_max = 0;
			int outDegree_min = 1;
			double avg = 0.0;
			int N_sum = 0;

//			if (key.equals("a")){
//
//				for (IntWritable val : values){
//					N_sum += val.get();
//				}
//					N_result.set(N_sum);
//					context.write(N_result,null);
//
//			}else{
				for (IntWritable val : values){
					edgesNum += val.get();
					N_sum += 1;
					if (val.get() > outDegree_max){
						outDegree_max = val.get();
					}
					if (val.get() < outDegree_min){
						outDegree_min = val.get();
					}

				}

//				for (IntWritable val : values){
//
//					N_sum += 1;
//
//				}
				avg = (double)edgesNum/ N_sum;
//			}


			edgesNum_result.set(Integer.toString(edgesNum));
			max_result.set(Integer.toString(outDegree_max));
			min_result.set(Integer.toString(outDegree_min));
			average_result.set(Double.toString(avg));
			context.write(new Text("total"), edgesNum_result);
			context.write(new Text("max"), max_result);
			context.write(new Text("min"), min_result);
			context.write(new Text("average"), average_result);
		}
	}

	public static class PageRankMapper
	extends Mapper<Object,// input key
	Text,  // input value
	Text,  // output key
	Text> //output value
	{
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String N = conf.get("N");    //passing parameters to Map
			String initialized = conf.get("init_status");
			if (initialized.equals("true")) {
				String[] parts = value.toString().split(" ", 3);
				if (parts.length == 2){
					context.write(new Text(parts[0]), new Text("$"));
				} else if (parts.length == 3){
					String[] p_neighbour = parts[2].split(" ");
					//maybe you can use another mark to distinguish between PR and neighbour list, start with $start with $
					context.write(new Text(parts[0]), new Text("$" + parts[2]));
					for (int p = 0; p < p_neighbour.length; p++){
						context.write(new Text(p_neighbour[p]), new Text(String.valueOf(Double.parseDouble(parts[1])/p_neighbour.length)));
					}
				}
				// old page rank, start with #
				context.write(new Text(parts[0]), new Text("#" + parts[1]));
			}else{
				// names not conflict in if and else, so you can use parts again in else
				String[] parts_1 = value.toString().split(" ", 2);
				double PR_part = 1 / Double.parseDouble(N);
				String neighbour = parts_1.length < 2 ? "" : " " + parts_1[1];
				context.write(new Text(parts_1[0]), new Text(Double.toString(PR_part) + neighbour));
			}
		}

	}

	// know how a mapper has changed a data item on a global level; one counter
	public enum Counters {
		CONVERGE_COUNTER, OUTPUT_COUNTER
	}

	public static class PageRankReducer
	//output value of reducer should be Text because it contains PR(double) and neighbour list(String)
	extends Reducer<Text,Text,Text,Text> {
		//result is IntWritable but sum should be double
		//int type will truncate the numbers after decimal point
		//for example  int a= 0.8 :   .8 will be discarded, so a = 0
		@Override
		public void reduce(Text key, Iterable<Text> values, //type of output value of mapper is Text
				Context context
				) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String N = conf.get("N");//passing parameters to Reduce
			String initialized = conf.get("init_status");
			if (initialized.equals("false")){
				context.write(key, values.iterator().next());
				return;
			}
			// For a node A, There are 3 kinds of value pass to the reducer
			// 1. Neighbour list of A(only one: either (A, null) or (A, the list)) starts with $
			// 2. Old PageRank of A(only one), starts with #
			// 3. PageRank of other nodes, used to calculate new PageRank of A(zero or more)
			double sum = 0.0;
			double oldPageRank = 0.0;
			String neighbourList = "";

			for (Text val : values) {
				if (val == null) continue;
				String vs = val.toString();
				if (vs.startsWith("#")) {
					// sub-string start from 2nd character, namely remove #
					oldPageRank = Double.parseDouble(vs.substring(1));
				} else if (vs.startsWith("$")) { // this is the neighbour list
					neighbourList = vs.length() > 1 ? " " + vs.substring(1) : ""; // discard $
				} else { // this is page rank from other nodes, add them up
					sum += Double.parseDouble(vs);
				}

			}
			// convert string to double : Double.parseDouble(value)
			double P_sum = (1-0.85)/Double.parseDouble(N) + 0.85 * sum;
			//add space between pagerank and neighbour, otherwise you can't split them next round
			context.write(key, new Text(Double.toString(P_sum) + neighbourList));

			if (Math.abs(oldPageRank - P_sum) < 1e-8){ // ^ in java doesn't means exponent,
				context.getCounter(Counters.CONVERGE_COUNTER).increment(1); //access the counter and increment it when we change to dataset
			}


		}

	}

	public static class SortMapper
	extends Mapper<Object,// input key
	Text,  // input value
	DoubleWritable,  // output key
	Text> //output value
	{
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] part = value.toString().split(" ", 2);
			String[] value_part = part[1].split(" ");
			context.write(new DoubleWritable(Double.parseDouble(value_part[0])), new Text(part[0]));
		}

	}

	public static class DescendingKeyComparator extends WritableComparator{
		protected DescendingKeyComparator(){
			super(DoubleWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleWritable key1 = (DoubleWritable) w1;
			DoubleWritable key2 = (DoubleWritable) w2;
			return -1 * key1.compareTo(key2);
		}
	}

	public static class SortReducer
	extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {

		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, //type of output value of mapper is Text
				Context context
				) throws IOException, InterruptedException {
			long globalCount = context.getCounter(Counters.OUTPUT_COUNTER).getValue();
			if(globalCount >= 10) {
				return;
			} else {
				int count = 0;

				for (Text val: values){
					context.write(val, key);
					if (count + globalCount >= 10){
						break;
					}else{
						count++;
					}
				}
				context.getCounter(Counters.OUTPUT_COUNTER).increment(count);
			}
		}

	}



	
	public static void CalNDriver(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRank.class);
		job.setMapperClass(CalNMapper.class);
		job.setReducerClass(CalNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}

	public static void CalEdgesDriver(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setNumReduceTasks(1);
		job.setJarByClass(PageRank.class);
		job.setMapperClass(CalEdgesMapper.class);
		job.setReducerClass(CalEdgesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}
	public static boolean PageRankDriver(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("N", N_sum); //conf.set: write into configuration file
		conf.set("init_status", initialized);
		conf.set("mapreduce.output.textoutputformat.separator", " ");
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		// output key and value of mapper
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

		// convergence
		long convergeCount = job.getCounters().findCounter(Counters.CONVERGE_COUNTER).getValue();
		if (convergeCount != oldConvergeCount) {
			oldConvergeCount = convergeCount;
			return true;
		} else {
			return false;
		}

	}

	public static void SortDriver(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setNumReduceTasks(1);
		job.setSortComparatorClass(DescendingKeyComparator.class);
		job.setJarByClass(PageRank.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}


	public static void main(String[] args) throws Exception {
		long begintime=0;
		long endtime=0;
		long costtime=0;
		begintime=System.nanoTime();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}
		String bucketName = otherArgs[1];
		String tmpDirName = bucketName + "tmp/";
		String resultDirName = bucketName + "result/";
		String calNOutput = tmpDirName + "ntmp/";
		String Nresult = resultDirName + "n.out";

		FileSystem fs = FileSystem.get(new URI(otherArgs[1]), conf);
		if (fs.exists(new Path(tmpDirName))){
			fs.delete(new Path(tmpDirName), true);
		}
		
		if (fs.exists(new Path(resultDirName))){
			fs.delete(new Path(resultDirName), true);
		}
		

		PageRank.CalNDriver(otherArgs[0], calNOutput);
		FileUtil.copyMerge(fs, new Path(calNOutput), fs, new Path(Nresult), false, conf, "");

		String calEdgesOutput = resultDirName + "Edges_Property/";
		String edgesProperty = resultDirName + "edgesProperty.out";
	
		PageRank.CalEdgesDriver(otherArgs[0], calEdgesOutput);
		FileUtil.copyMerge(fs, new Path(calEdgesOutput), fs, new Path(edgesProperty), false, conf, "");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Nresult))));
		N_sum = br.readLine();
//		String line = null;
//		while ((line = br.readLine()) != null) {
//			String[] RL = line.split("\t"); //read a file from HDFS in Hadoop
//			if (RL[0] == "&"){
//				N_sum = RL[1];
//			}else{
//				edges_sum = RL[1];
//			}
//		}



		PageRank.PageRankDriver(otherArgs[0], tmpDirName + "iter0");
		int iterCount = 0;
		initialized = "true";
		boolean iterate = true;
		while (iterate) {
			iterate = PageRank.PageRankDriver(tmpDirName + "iter" + iterCount, tmpDirName + "iter" + (iterCount + 1));
			iterCount++;
		}
		String sortOutput = tmpDirName + "sorttmp";
		String sortResult = resultDirName + "pagerank.out";
		PageRank.SortDriver(tmpDirName + "iter" + iterCount, sortOutput);
		FileUtil.copyMerge(fs, new Path(sortOutput), fs, new Path(sortResult), false, conf, "");
		endtime=System.nanoTime();
		costtime=(endtime-begintime)/1000/1000;
		System.out.println("The execution time is: "+costtime+"ms");
	}
}
