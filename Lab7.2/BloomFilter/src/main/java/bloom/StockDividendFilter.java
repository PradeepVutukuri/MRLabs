package bloom;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class StockDividendFilter extends Configured implements Tool {
	private static final String FILTER_FILE = "filters/dividendfilter";

	public static class BloomMapper extends Mapper<LongWritable, Text, NullWritable, BloomFilter> {
		private String stockSymbol;
		private BloomFilter outputValue;
		
		private NullWritable outputKey = NullWritable.get();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stockSymbol = context.getConfiguration().get("stockSymbol");
			outputValue = new BloomFilter(1000, 20, Hash.MURMUR_HASH);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = StringUtils.split(value.toString(),'\\',',');
			if(words[1].equals(stockSymbol)) {
				Stock stock = new Stock(words[1], words[2]);
				Key stockKey = new Key(stock.toString().getBytes());
				outputValue.add(stockKey);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(outputKey, outputValue);
		}	
	}
	
	public static class BloomReducer extends Reducer<NullWritable, BloomFilter, NullWritable, NullWritable> {
		private BloomFilter allValues;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			allValues = new BloomFilter(1000,20,Hash.MURMUR_HASH);
		}

		@Override
		protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context)
				throws IOException, InterruptedException {			
			for(BloomFilter filter : values) {
				allValues.or(filter);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			Path path = new Path(FILTER_FILE);
			FSDataOutputStream out = path.getFileSystem(conf).create(path);
			allValues.write(out);
			out.close();
		}
	}
	
	
	public static class StockFilterMapper extends Mapper<LongWritable, Text, Stock, DoubleWritable> {
		private BloomFilter dividends;
		private String stockSymbol;
		private DoubleWritable outputValue = new DoubleWritable();
		Stock outputKey = new Stock();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path [] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			stockSymbol = context.getConfiguration().get("stockSymbol");
			
			if(files.length > 0) {
				//Initialize the dividends field
				dividends = new BloomFilter(1000, 20, Hash.MURMUR_HASH);
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FSDataInputStream in = fs.open(files[0]);
				dividends.readFields(in);
				in.close();
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] words = StringUtils.split(value.toString(),'\\',',');
			if(words[1].equals(stockSymbol)) {
				outputKey.setSymbol(words[1]);
				outputKey.setDate(words[2]);
				Key stockKey = new Key(outputKey.toString().getBytes());
				if(dividends.membershipTest(stockKey)) {
					outputValue.set(Double.parseDouble(words[6]));
					context.write(outputKey, outputValue);
				}
			}
		}
	}

	public static class StockFilterReducer extends Reducer<Stock, DoubleWritable, Text, DoubleWritable> {
		private Text outputKey = new Text();

		@Override
		protected void reduce(Stock key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			for(DoubleWritable value : values) {
				outputKey.set(key.toString());
				context.write(outputKey, value);
			}
		}

	}


	@Override
	public int run(String[] args) throws Exception {
		Job job1 = new Job(getConf(), "CreateBloomFilter");
		job1.setJarByClass(getClass());
		Configuration conf = job1.getConfiguration();
		conf.set("stockSymbol", args[0]);

		FileInputFormat.setInputPaths(job1, new Path("dividends"));
		
		job1.setMapperClass(BloomMapper.class);
		job1.setReducerClass(BloomReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(NullOutputFormat.class);
		job1.setMapOutputKeyClass(NullWritable.class);
		job1.setMapOutputValueClass(BloomFilter.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(NullWritable.class);
		job1.setNumReduceTasks(1);
		
		boolean job1success = job1.waitForCompletion(true);
		if(!job1success) {
			System.out.println("The CreateBloomFilter job failed!");
			return -1;
		}

		Job job2 = new Job(conf, "FilterStocksJob");
		job2.setJarByClass(getClass());
		conf = job2.getConfiguration();

		Path out = new Path("bloomoutput");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job2, new Path("stocks"));
		FileOutputFormat.setOutputPath(job2, out);
		
		job2.setMapperClass(StockFilterMapper.class);
		job2.setReducerClass(StockFilterReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapOutputKeyClass(Stock.class);
		job2.setMapOutputValueClass(DoubleWritable.class);	
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		DistributedCache.addCacheFile(new URI(FILTER_FILE), conf);
		
		boolean job2success = job2.waitForCompletion(true);
		if(!job2success) {
			System.out.println("The FilterStocksJob failed!");
			return -1;
		}		
		return 1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new StockDividendFilter(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
