package org.way2bigdata.avro.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.way2bigdata.avro.schema.Employee;

/**
 * Hello world!
 *
 */
public class EmployeeDemo {
	
	private static final String AVRO_DATA_FILE_PATH = "C:/Users/way2bigdata_2/employee.avro";
    public static void main( String[] args ) throws IOException
    {
    	
    	// by using set methods
    	Employee emp1 = new Employee();
    	emp1.setEmpId(1);
    	emp1.setName("jack");
    	
    	// by using constructor
    	Employee emp2 = new Employee("jim", 2);
    	
    	// by using string builder
    	Employee emp3 = Employee.newBuilder()
    			.setEmpId(3)
    			.setName("john")
    			.build();
    	
    	EmployeeUtil.serialize(emp1,emp2,emp3, AVRO_DATA_FILE_PATH);
    	
    	EmployeeUtil.deSerialize(AVRO_DATA_FILE_PATH);
    	

    }
}
