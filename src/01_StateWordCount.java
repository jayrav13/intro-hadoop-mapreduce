/**
 *	StateWordCount
 *
 *	This MapReduce Job determines the number of times one of {"education", "politics", "sports", "agriculture"}
 *	appears in each state's Wikipedia page.
 */

// Imports
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *	StateWordCount
 */
public class StateWordCount {

	// Mapper Class
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		// IntWritable will set the context.write(...)'s value.
		private final static IntWritable one = new IntWritable(1);

		// Track the current word as a Text() object.
		private Text word = new Text();

		// These are the key words to search for.
		String[] search = new String[] {"education", "politics", "sports", "agriculture"};

		// Map function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Convert value to StringTokenizer.
			StringTokenizer itr = new StringTokenizer(value.toString());

			// Get the current file's path name.
			String filepath = ((FileSplit) context.getInputSplit()).getPath().toString();

			// Iterate through StringTokenizer.
			while (itr.hasMoreTokens()) {

				// Retrieve current word in line.
				word.set(itr.nextToken().toLowerCase());

				// Iterate through all possible search terms.
				for(int i = 0; i < search.length; i++) {

					// Continue to iterate until this "word", or string of characters,
					// no longer has any more instances of any of the search words.
					while(word.toString().contains(search[i])) {
						context.write(new Text(filepath + "/" + search[i]), one);
						word.set(word.toString().replaceFirst(search[i], ""));
					}
				}
			}
		}
	}

	// Reducer Class
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

		// Set up IntWritable
		private IntWritable result = new IntWritable();

		// Reduce function.
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			String tmp = key.toString().toLowerCase();

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// Main function.
	public static void main(String[] args) throws Exception {

		// Create new config object.
		Configuration conf = new Configuration();

		// Set up a new job and give it a name.
		Job job = Job.getInstance(conf, "word count");

		// When creating jar, use this class name.
		job.setJarByClass(StateWordCount.class);

		// Class for mapping.
		job.setMapperClass(TokenizerMapper.class);

		// Class for combining.
		job.setCombinerClass(IntSumReducer.class);

		// Class for reducing.
		job.setReducerClass(IntSumReducer.class);

		// What object type is the output key.
		job.setOutputKeyClass(Text.class);

		// What object type is the output value.
		job.setOutputValueClass(IntWritable.class);

		// Set input and output paths.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Exit when done.
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
