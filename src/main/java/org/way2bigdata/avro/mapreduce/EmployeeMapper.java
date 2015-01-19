package org.way2bigdata.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.way2bigdata.avro.schema.Employee;

import com.google.common.base.Preconditions;

public class EmployeeMapper extends Mapper<AvroKey<Employee>,NullWritable,Text,IntWritable>{
	
	public void map(AvroKey<Employee> key, NullWritable value, Context context) throws IOException, InterruptedException{
		
		Preconditions.checkNotNull(key);
		
		Employee emp  = null;
			
			 emp = key.datum();
			 
			 String name = emp.getName().toString();
			 Integer id = emp.getEmpId();
			 
			 context.write(new Text(name), new IntWritable(id));
	}
}
