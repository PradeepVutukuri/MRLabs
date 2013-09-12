package customsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DividendJob extends Configured implements Tool {

	
	public static class StockPartitioner extends Partitioner<Stock, DoubleWritable> {

		@Override
		public int getPartition(Stock key, DoubleWritable value, int numPartitions) {
			char firstLetter = key.getSymbol().trim().charAt(0);
			return (firstLetter - 'A') % numPartitions;
		}
	}

	public static class DividendGrowthMapper extends Mapper<LongWritable, Text, Stock, DoubleWritable> {
		private Stock outputKey = new Stock();
		private DoubleWritable outputValue = new DoubleWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = value.toString().split(",");
			if(words[0].equals("exchange")) return;
			
			outputKey.setSymbol(words[1]);
			outputKey.setDate(words[2]);
			outputValue.set(Double.parseDouble(words[3]));
			context.write(outputKey, outputValue);
		}
	}
	
	public static class DividendGrowthReducer extends Reducer<Stock, DoubleWritable, NullWritable, DividendChange> {
		private NullWritable outputKey = NullWritable.get();
		protected DividendChange outputValue = new DividendChange();
		protected Stock previousStock = new Stock();
		protected double previousPrice = 0.0;
		
		@Override
		protected void reduce(Stock key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			if(key.getSymbol().equals(previousStock.getSymbol())) {
				//The prior stock is the same as this one
				double currentPrice = values.iterator().next().get();
				double growth = currentPrice - previousPrice;
				if(growth != 0.0) {
					outputValue.setSymbol(key.getSymbol());
					outputValue.setDate(key.getDate());
					outputValue.setChange(growth);
					context.write(outputKey,outputValue);
					previousPrice = currentPrice;
				}				
			} else {
				//previousStock = key;  //This is a catastrophe - a new key object is not instantiated for each key/value pair that comes in
				previousStock.setSymbol(key.getSymbol());
				previousPrice = values.iterator().next().get();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = new Job(conf, "DividendJob");
		job.setJarByClass(DividendJob.class);
		
		Path out = new Path("growth");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job, new Path("dividends"));
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(DividendGrowthMapper.class);
		job.setReducerClass(DividendGrowthReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DividendChange.class);
		job.setMapOutputKeyClass(Stock.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setPartitionerClass(StockPartitioner.class);
		
		job.setNumReduceTasks(26);

		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new DividendJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
