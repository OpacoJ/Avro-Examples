package org.way2bigdata.avro.no_code_gen;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.way2bigdata.avro.examples.EmployeeUtil;

public class NoCodeGenDemo {
	
	public static final String OUTPUT_AVRO_FILE_PATH = "C:/Users/way2bigdata_2/employee.avro";
	
	public static void main(String args[]) throws IOException{
		
		// Read the schema by parsing it and create a schema object.
		Schema schema = new Schema.Parser().parse(new File("C:/Users/way2bigdata_2/workspace/Avro-Examples/src/main/avro/employee.avsc"));
		        
		// create an employee avro object using schema
		GenericRecord emp1 = new GenericData.Record(schema);
		emp1.put("name", "jim");
		emp1.put("emp_id", 1);

		GenericRecord emp2 = new GenericData.Record(schema);
		emp2.put("name", "jack");
		emp2.put("emp_id", 2);
		
		GenericRecord emp3 = new GenericData.Record(schema);
		emp3.put("name", "john");
		emp3.put("emp_id", 3);
		
		EmployeeUtil.serialize(emp1, emp2, emp3, schema , OUTPUT_AVRO_FILE_PATH);
		
		EmployeeUtil.deSerialize(OUTPUT_AVRO_FILE_PATH, schema);
	}

}
