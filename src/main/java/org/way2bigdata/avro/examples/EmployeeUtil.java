package org.way2bigdata.avro.examples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.tether.OutputProtocol;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.way2bigdata.avro.schema.Employee;

public class EmployeeUtil {
	
	public static void serialize(Employee e1, Employee e2, Employee e3, String outputPath) throws IOException{
		
		DatumWriter<Employee> writer = new SpecificDatumWriter<Employee>(Employee.class);
		DataFileWriter<Employee> fileWriter = new DataFileWriter<Employee>(writer);
		fileWriter.create(e1.getSchema(), new File(outputPath));
		
		fileWriter.append(e1);
		fileWriter.append(e2);
		fileWriter.append(e3);
		fileWriter.close();
		
	}
	
	public static void serialize(GenericRecord e1, GenericRecord e2, GenericRecord e3, Schema schema, String outputPath) throws IOException{
		
		DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(writer);
		fileWriter.create(schema, new File(outputPath));
		
		fileWriter.append(e1);
		fileWriter.append(e2);
		fileWriter.append(e3);
		fileWriter.close();
		
	}
	
	
	public static void deSerialize(String path) throws IOException{
		
		DatumReader<Employee> reader = new SpecificDatumReader<Employee>(Employee.class);
		
		File file = new File(path);
		DataFileReader<Employee> fileReader = new DataFileReader<Employee>(file, reader);
		
		while (fileReader.hasNext()){
			Employee e = null;
			e = fileReader.next();
			System.out.println(e);
		}
	}
	
	public static void deSerialize(String path, Schema schema) throws IOException{
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		
		File file = new File(path);
		DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file, reader);
		
		while (fileReader.hasNext()){
			GenericRecord e = null;
			e = fileReader.next();
			System.out.println(e);
		}
	}
}
