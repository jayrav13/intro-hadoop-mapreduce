/**
 *	ProblemOne
 *
 *	This is a chained set of jobs in MapReduce.
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
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import java.util.regex.Pattern;
import java.util.*;

public class ProblemTwo {

	// Mapper Class
	public static class FullDocumentMapper extends Mapper<Object, Text, Text, IntWritable>{

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
	public static class FullDocumentReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

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

	// Mapper Class
	public static class ManipulationMapper extends Mapper<Object, Text, Text, Text>{

		// IntWritable will set the context.write(...)'s value.
		private final static IntWritable one = new IntWritable(1);

		// Track the current word as a Text() object.
		private Text word = new Text();

		// These are the key words to search for.
		String[] search = new String[] {"education", "politics", "sports", "agriculture"};

		// Map function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Store split value of the string.
			String[] values = new String[] {"", ""};

			// Count as the string has more tokens.
			int counter = 0;

			// Tokenize, iterate.
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {

				values[counter] = st.nextToken();
				counter += 1;

			}
			counter = 0;

			// Substrings for state and phrase.
			int lastElemIndex = values[0].lastIndexOf("/");
			String state = values[0].substring(0, lastElemIndex);
			String phrase = values[0].substring(lastElemIndex + 1, values[0].length());

			// int count = Integer.parseInt(values[1]);

			StringBuilder sb = new StringBuilder();
			sb.append(phrase).append("-").append(values[1]);

			context.write(new Text(state), new Text(sb.toString()));

		}
	}

	// Reducer Class
	public static class ManipulationReducer extends Reducer<Text,Text,Text,Text> {

		// Reduce function.
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Store results.
			ArrayList<Integer> ints = new ArrayList<Integer>();
			Map<Integer, String> map = new HashMap<Integer, String>();
			ArrayList<String> str = new ArrayList<String>();

			// Iterate through a state's phrases.
			for (Text val : values) {
				str.add(val.toString());
			}

			int index = 0;
			int count = -1;
			for(int i = 0; i < str.size(); i++) {
				ints.add( Integer.parseInt( str.get(i).split("-")[1] ) );
				map.put( Integer.parseInt( str.get(i).split("-")[1] ), str.get(i).split("-")[0] );
			}

			Collections.sort( ints );
			StringBuilder rebuild = new StringBuilder();

			for(int i = 0; i < ints.size(); i++) {
				//rebuild.append( ints.get(i) )
				context.write( new Text(str.get(index)), new Text( ints.get(i).toString() ) );
			}
		}
	}

	// Main function.
	public static void main(String[] args) throws Exception {

		// Create new config object.
		Configuration conf = new Configuration();

		/**
		 *	Job 1
		 */
		// Set up a new job and give it a name.
		Job job = Job.getInstance(conf, "Full Document Count");

		// When creating jar, use this class name.
		job.setJarByClass(ProblemTwo.class);

		// Class for mapping.
		job.setMapperClass(FullDocumentMapper.class);

		// Class for combining.
		job.setCombinerClass(FullDocumentReducer.class);

		// Class for reducing.
		job.setReducerClass(FullDocumentReducer.class);

		// What object type is the output key.
		job.setOutputKeyClass(Text.class);

		// What object type is the output value.
		job.setOutputValueClass(IntWritable.class);

		// Set input and output paths.

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path( "/project/problems/two/full_document_count" ));

		// Exit when done.
		job.waitForCompletion(true);

		/**
		 *	Job 2
		 */
		// Set up a new job and give it a name.
		Job job2 = Job.getInstance(conf, "Manipulation Job");

		// When creating jar, use this class name.
		job2.setJarByClass(ProblemTwo.class);

		// Class for mapping.
		job2.setMapperClass(ManipulationMapper.class);

		// Class for combining.
		job2.setCombinerClass(ManipulationReducer.class);

		// Class for reducing.
		job2.setReducerClass(ManipulationReducer.class);

		// What object type is the output key.
		job2.setOutputKeyClass(Text.class);

		// What object type is the output value.
		job2.setOutputValueClass(Text.class);

		// Set input and output paths.
		FileInputFormat.addInputPath(job2, new Path( "/project/problems/two/full_document_count"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		// Exit when done.
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

}