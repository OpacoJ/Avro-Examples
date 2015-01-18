package org.way2bigdata.avro.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.way2bigdata.avro.examples.EmployeeUtil;
import org.way2bigdata.avro.schema.Employee;

public class JDBCClient {
	
	private static final String URL = "jdbc:mysql://localhost/world";
	private static final String USER="root";
	private static final String PASSWORD="mysql";
	private static final String AVRO_DATA_FILE_PATH = "C:/Users/way2bigdata_2/employee.avro";
	
	public static void main(String args[]) throws SQLException, IOException{
		
		JDBCUtil.registerDriver();
		
		Connection conn = JDBCUtil.openConnection(URL, USER, PASSWORD);
		
		Statement stmt = JDBCUtil.createStatement(conn);
		
		String sql = "select * from employee";
		ResultSet rs = JDBCUtil.executeQuery(sql, stmt);
		
        Employee[] empArray = new Employee[3];
		 int count=0;

			while(rs.next()){
		         //retrive fields from result set
		         int id  = rs.getInt("emp_id");
		         String name = rs.getString("empname");
		         
		         Employee emp = new Employee();
		         emp.setEmpId(id);
		         emp.setName(name);
		         
		         System.out.println(emp.getEmpId());
		         System.out.println(emp.getName());
		         
		         empArray[count]=emp;
		         System.out.println(empArray[count]);
		         count++;
		      }
			
			EmployeeUtil.serialize(empArray[0],empArray[1],empArray[2], AVRO_DATA_FILE_PATH);
	}

}
