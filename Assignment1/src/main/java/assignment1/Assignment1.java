package assignment1;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * 
 * This class solves the problem posed for Assignment1
 *
 */

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Assignment1 {

// TODO: Write the source code for your solution here.	

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntTextPair> {

		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		private Configuration conf;
		private int ngram;
		private Text filename = new Text();

		// get ngram parameter from configuration and filename from filesplit class
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			ngram = Integer.parseInt(conf.get("ngram"));
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filename.set(fileSplit.getPath().getName());
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// split ngram-words from document by passing it to NgramIterator class.
			NgramIterator itr = new NgramIterator(value.toString(), ngram);
			while (itr.hasNext()) {
				word.set(itr.next());
				context.write(word, new IntTextPair(one, filename));
			}
		}
	}

	public static class TextConcatCombiner extends Reducer<Text, IntTextPair, Text, IntTextPair> {
		private IntTextPair result = new IntTextPair();

		public void reduce(Text key, Iterable<IntTextPair> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			// Creating a set of filenames
			Set<String> file_Set = new HashSet<String>();
			
			// iterate through all values with respect to a key, sum up number of word and add filename to set
			for (IntTextPair val : values) {
				sum += val.getFirst().get();
				// split value in case of text is a list of filenames.
				StringTokenizer itr = new StringTokenizer(val.getSecond().toString(),":");
				while (itr.hasMoreTokens()) {
					file_Set.add(itr.nextToken());
				}
			}

			// Sorting filename list by passing it to tree set.
			Set<String> sorted_file_Set = new TreeSet<String>(file_Set);
			
			// Creating a filenames string from sorted filename list.
			Iterator<String> file_name_itr = sorted_file_Set.iterator();
			StringBuilder file_list_sb = new StringBuilder();
			while (file_name_itr.hasNext()) {
				file_list_sb.append((file_list_sb.length() == 0 ? "" : ":") + file_name_itr.next());
			}

			// Form the final key/value pairs result for each word using context
			result.set(new IntWritable(sum), new Text(file_list_sb.toString()));
			context.write(key, result);

		}
	}

	public static class TextConcatReducer extends Reducer<Text, IntTextPair, Text, IntTextPair> {
		private IntTextPair result = new IntTextPair();

		public void reduce(Text key, Iterable<IntTextPair> values, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			// Get min_count argument
			int min_count = Integer.parseInt(conf.get("min_count"));
			int sum = 0;

			// Creating a set of filenames
			Set<String> file_Set = new HashSet<String>();
			
			// iterate through all values with respect to a key, sum up number of word and add filename to set
			for (IntTextPair val : values) {
				sum += val.getFirst().get();
				StringTokenizer itr = new StringTokenizer(val.getSecond().toString(),":");
				while (itr.hasMoreTokens()) {
					file_Set.add(itr.nextToken());
				}
			}

			// terminate reducer if sum is less than min_count
			if (sum < min_count)
				return;

			// Sorting filename list by passing it to tree set.
			Set<String> sorted_file_Set = new TreeSet<String>(file_Set);
			
			// Creating a filenames string from sorted filename list.
			Iterator<String> file_name_itr = sorted_file_Set.iterator();
			StringBuilder file_list_sb = new StringBuilder();
			while (file_name_itr.hasNext()) {
				file_list_sb.append((file_list_sb.length() == 0 ? "" : " ") + file_name_itr.next());
			}

			// Form the final key/value pairs result for each word using context
			result.set(new IntWritable(sum), new Text(file_list_sb.toString()));
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception {
		// Creating a Configuration object and a Job object, assigning a job name for identification purposes
		Configuration conf = new Configuration();
		
		// Creating a GenericOptionsParser, set ngram and min_count argument to configuration
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if (remainingArgs.length != 4) {
			System.err
					.println("Usage: assignment1 <value_of_ngram> <minimum_count_of_ngram> <input_path> <output_path>");
			System.exit(2);
		}
		conf.set("ngram", remainingArgs[0]);
		conf.set("min_count", remainingArgs[1]);
		
		Job job = Job.getInstance(conf, "assignment1");

		// Setting the job's jar file by finding the provided class location
		job.setJarByClass(Assignment1.class);
		
		// Providing the mapper, combiner and reducer class names
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(TextConcatCombiner.class);
		job.setReducerClass(TextConcatReducer.class);
		
		// Setting configuration object with the Data Type of output Key and Value for map and reduce
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntTextPair.class);
		
		// input and output directory to be fetched from the command line
		FileInputFormat.addInputPath(job, new Path(remainingArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[3]));
		
		// Submit the job to the cluster and wait for it to finish 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class NgramIterator implements Iterator<String> {

	String[] words;
	int pos = 0, n;

	public NgramIterator(String s, int n) {
		this.n = n;
		this.words = s.split(" ");
	}

	public boolean hasNext() {
		// is it has next ngram-word
		return pos < this.words.length - this.n + 1;
	}

	public String next() {
		// get next ngram-word from a string
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < this.n; i++) {
			str.append((i == 0 ? "" : " ") + this.words[this.pos + i]);
		}
		this.pos++;
		return str.toString();
	}

}

class IntTextPair implements Writable {

	private IntWritable first;
	private Text second;

	public IntTextPair() {
		this.first = new IntWritable(0);
		this.second = new Text();
	}

	public IntTextPair(IntWritable first, Text second) {
		this.first = first;
		this.second = second;
	}

	public IntWritable getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public void setFirst(IntWritable first) {
		this.first = first;
	}

	public void setSecond(Text second) {
		this.second = second;
	}

	public void set(IntWritable first, Text second) {
		this.first = first;
		this.second = second;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(arg0);
		second.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		first.write(arg0);
		second.write(arg0);
	}

	@Override
	public String toString() {
		return first.get() + "  " + second.toString();
	}

}
