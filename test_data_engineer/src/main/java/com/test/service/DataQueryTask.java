package com.test.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DataQueryTask {

	@Autowired
	@Qualifier("springJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	public void createEmployeeTable() {
		
		try {
	        jdbcTemplate.execute("CREATE TABLE Employee (" +
	                "Id INTEGER PRIMARY KEY AUTO_INCREMENT," +
	                "EmployeeId VARCHAR(10) UNIQUE NOT NULL," +
	                "FullName VARCHAR(100) NOT NULL," +
	                "BirthDate DATE NOT NULL," +
	                "Address VARCHAR(500)" +
	                ")");
		} catch (Exception e) {
			
		}
    }
	
	public void insertEmployeeData() {
		
		try {
	        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
	        Date birthDate1 = dateFormat.parse("19-Aug-82");
	        Date birthDate2 = dateFormat.parse("1-Jan-82");
	        Date birthDate3 = dateFormat.parse("20-Feb-82");
	        Date birthDate4 = dateFormat.parse("22-Feb-82");
	        
	        jdbcTemplate.update("INSERT INTO Employee (Id, EmployeeId, FullName, BirthDate, Address) " +
	                "VALUES (1, '10105001', 'Ali Anton', ?, 'Jakarta Utara'), " +
	                "(2, '10105002', 'Rara Siva', ?, 'Mandalika'), " +
	                "(3, '10105003', 'Rini Aini', ?, 'Sumbawa Besar'), " +
	                "(4, '10105004', 'Budi', ?, 'Mataram Kota')",
	                birthDate1, birthDate2, birthDate3, birthDate4);
	        
		} catch (Exception e) {
			
		}
	}
	
	public void createPositionHistoryTable() {
		
		try {
	        jdbcTemplate.execute("CREATE TABLE Position_History (" +
	                "Id INTEGER PRIMARY KEY AUTO_INCREMENT," +
	                "PosId VARCHAR(10) NOT NULL," +
	                "PosTitle VARCHAR(100) NOT NULL," +
	                "EmployeeId VARCHAR(10) NOT NULL," +
	                "StartDate DATE NOT NULL" +
	                "EndDate DATE NOT NULL" +
	                ")");
		} catch (Exception e) {
			
		}
    }
	
	public void insertPositionHistoryData() {
		
		try {
	        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
	        Date startDate1 = dateFormat.parse("1-Jan-2022");
	        Date startDate2 = dateFormat.parse("1-Mar-2022");
	        Date startDate3 = dateFormat.parse("1-Jan-2022");
	        Date startDate4 = dateFormat.parse("1-Mar-2022");
	        Date startDate5 = dateFormat.parse("1-Jan-2022");
	        Date startDate6 = dateFormat.parse("1-Mar-2022");
	        
	        Date endDate1 = dateFormat.parse("28-Feb-2022");
	        Date endDate2 = dateFormat.parse("31-Dec-2022");
	        Date endDate3 = dateFormat.parse("28-Feb-2022");
	        Date endDate4 = dateFormat.parse("31-Dec-2022");
	        Date endDate5 = dateFormat.parse("28-Feb-2022");
	        Date endDate6 = dateFormat.parse("31-Dec-2022");
	        
	        jdbcTemplate.update("INSERT INTO Employee (Id, PosId, PosTitle, EmployeeId, StartDate, EndDate) " +
	                "VALUES "
	                + "(1, '50000', 'IT Manager', '10105001', ?, ?), "
	                + "(2, '50001', 'IT Sr. Manager', '10105001', ?, ?), "
	                + "(3, '50002', 'Programmer Analyst', '10105002', ?, ?), "
	                + "(4, '50003', 'Sr. Programmer Analyst', '10105002', ?, ?), "
	                + "(5, '50004', 'IT Admin', '10105003', ?, ?), "
	                + "(6, '50005', 'IT Secretary', '10105003', ?, ?)",
	                startDate1, endDate1, startDate2, endDate2, startDate3, endDate3, startDate4, endDate4, 
	                startDate5, endDate5, startDate6, endDate6);
	        
		} catch (Exception e) {
			
		}
	}
	
	public List<HashMap<String, Object>> getAllEmployeeWithCurrentPos() {
		
		List<HashMap<String, Object>> listParam = new ArrayList<HashMap<String, Object>>();
		
		String query = "SELECT DISTINCT e.EmployeeId, e.FullName, e.BirthDate, e.Address, ph.PosId, ph.PosTitle, ph.StartDate, ph.EndDate "
				+ "FROM Employee e "
				+ "INNER JOIN Position_History ph ON e.EmployeeId = ph.EmployeeId "
				+ "ORDER BY e.EmployeeId, ph.StartDate DESC";

		Connection con = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		
		try {
			con = jdbcTemplate.getDataSource().getConnection();
			ps = con.prepareStatement(query);
			rs = ps.executeQuery();
			
			while (rs.next() && rs != null){
				
				HashMap<String, Object> parameter = new HashMap<String, Object>();
				parameter.put("EmployeeId", rs.getString("EmployeeId").trim());
				parameter.put("FullName", rs.getString("FullName").trim());
				parameter.put("BirthDate", rs.getString("BirthDate").trim());
				parameter.put("Address", rs.getString("Address").trim());
				parameter.put("PosId", rs.getString("PosId").trim());
				parameter.put("PosTitle", rs.getString("PosTitle").trim());
				parameter.put("StartDate", rs.getDate("StartDate"));
				parameter.put("EndDate", rs.getDate("EndDate"));
				
				listParam.add(parameter);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		
		} finally {
			try {
				rs.close();
			} catch (Exception e2) {}
			try {
				ps.close();
			} catch (Exception e) {}
			try {
				con.close();
			} catch (Exception e) {}
		}
		
		return listParam;
	}

}
