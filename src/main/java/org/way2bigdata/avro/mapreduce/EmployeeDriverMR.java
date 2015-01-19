package org.way2bigdata.avro.mapreduce;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.way2bigdata.avro.schema.Employee;

public class EmployeeDriverMR extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {


		if (args.length < 2) {
		      System.err.println("Usage: EmployeeDriverMR <input path> <output path>");
		      return -1;
		    }

		    Job job = new Job(getConf());
		    job.setJarByClass(EmployeeDriverMR.class);
		    job.setJobName("Avro Employee");
		    
		    FileInputFormat.setInputPaths(job, args[0] );
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    job.setInputFormatClass(AvroKeyInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);

		    job.setMapperClass(EmployeeMapper.class);
		    AvroJob.setInputKeySchema(job, Employee.getClassSchema());
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);

		    return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("input" + args[0]);
		System.out.println("output" +args[1]);
	    int res = ToolRunner.run(new EmployeeDriverMR(), args);
	    System.exit(res);
	  }

}
