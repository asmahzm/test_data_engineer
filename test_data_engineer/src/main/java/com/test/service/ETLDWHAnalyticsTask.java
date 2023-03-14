package com.test.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.opencsv.CSVWriter;

@Service
public class ETLDWHAnalyticsTask {

	@Autowired
	@Qualifier("springJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	public List<HashMap<String, Object>> getAzureEmployeeData() {
		
		List<HashMap<String, Object>> employeeList = new ArrayList<HashMap<String, Object>>();
		
		String query = "SELECT * FROM Employee";

		Connection con = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		
		try {
			con = jdbcTemplate.getDataSource().getConnection();
			ps = con.prepareStatement(query);
			rs = ps.executeQuery();
			
			while (rs.next() && rs != null){
				
				HashMap<String, Object> parameter = new HashMap<String, Object>();
				parameter.put("EmployeeNumber", rs.getString("EmployeeNumber").trim());
				parameter.put("FullName", rs.getString("FullName").trim());
				parameter.put("BirthDate", rs.getString("BirthDate").trim());
				parameter.put("Position", rs.getString("Position").trim());
				
				employeeList.add(parameter);
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
		
		return employeeList;
	}
	
	public List<HashMap<String, Object>> getTrainingHistoryData() {
		
		try {
			GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream("path/to/credentials.json"))
			        .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS_READONLY));
	
			Sheets sheetsService = new Sheets.Builder(GoogleNetHttpTransport.newTrustedTransport(), 
			        JacksonFactory.getDefaultInstance(), credential)
			        .setApplicationName("Training History Data")
			        .build();
			
			String spreadsheetId = "your-spreadsheet-id";
			String range = "Training Data!A1:F";
			ValueRange response = sheetsService.spreadsheets().values()
			        .get(spreadsheetId, range)
			        .execute();
			List<List<Object>> values = response.getValues();

			List<HashMap<String, Object>> trainingHistoryList = new ArrayList<HashMap<String, Object>>();
			if (values == null || values.isEmpty()) {
			    System.out.println("No data found.");
			} else {
			    for (List<Object> row : values) {
			        // Map each row of data to a TrainingHistory object
			    	HashMap<String, Object> trainingHistory = new HashMap<String, Object>();
			        trainingHistory.put("EmployeeNumber", row.get(0).toString());
			        trainingHistory.put("TrainingName", row.get(1).toString());
			        trainingHistory.put("TrainingDate", Date.valueOf(row.get(2).toString()));
			        trainingHistory.put("TrainingLocation", row.get(3).toString());
			        trainingHistoryList.add(trainingHistory);
			    }
			}
			
			return trainingHistoryList;

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public void insertDataToDWH(List<HashMap<String, Object>> employeeList, List<HashMap<String, Object>> trainingHistoryList) {
		
		try {
	        jdbcTemplate.execute("CREATE TABLE Employee (" +
	                "Id INTEGER PRIMARY KEY AUTO_INCREMENT," +
	                "EmployeeNumber VARCHAR(10) UNIQUE NOT NULL," +
	                "FullName VARCHAR(100) NOT NULL," +
	                "BirthDate DATE NOT NULL," +
	                "Position VARCHAR(100) NOT NULL," +
	                "TrainingName VARCHAR(100) NOT NULL," +
	                "TrainingDate DATE NOT NULL," +
	                "TrainingLocation VARCHAR(100) NOT NULL" +
	                ")");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			for (HashMap<String, Object> employee : employeeList) {
				for (HashMap<String, Object> trainingHistory :trainingHistoryList) {
					if ((employee.get("EmployeeNumber").toString()).equals(trainingHistory.get("EmployeeNumber").toString())) {
						jdbcTemplate.update("INSERT INTO EmployeeDWH (EmployeeNumber, FullName, BirthDate, Position, "
							+ "TrainingName, TrainingDate, TrainingLocation) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?)",
			                employee.get("EmployeeNumber"), employee.get("FullName"), employee.get("BirthDate"), 
			                employee.get("Position"), trainingHistory.get("TrainingName"), trainingHistory.get("TrainingDate"), 
			                trainingHistory.get("TrainingLocation"));
						
					} else {
						
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void reportTrainingHistoryData() {
		
		try {
			// Google Worksheet
			GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream("path/to/credentials.json"))
			        .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS_READONLY));
	
			Sheets sheetsService = new Sheets.Builder(GoogleNetHttpTransport.newTrustedTransport(), 
			        JacksonFactory.getDefaultInstance(), credential)
			        .setApplicationName("Training History Data")
			        .build();
			
			String spreadsheetId = "your-spreadsheet-id";
			String range = "Training Data!A1:F";
			ValueRange response = sheetsService.spreadsheets().values()
			        .get(spreadsheetId, range)
			        .execute();
			List<List<Object>> values = response.getValues();

		    // Create OpenCSV file
		    File file = new File("/path/to/output.csv");
		    FileWriter outputfile = new FileWriter(file);
		    CSVWriter writer = new CSVWriter(outputfile);

		    List<String[]> data = new ArrayList<String[]>();
		    for (List<Object> row : values) {
		        String[] rowData = new String[row.size()];
		        int i = 0;
		        for (Object cell : row) {
		            rowData[i] = cell.toString();
		            i++;
		        }
		        data.add(rowData);
		    }
		    writer.writeAll(data);

		    writer.close();
		    outputfile.close();
		   
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void dashboardTraining() {
		
		String sql_1 = "SELECT COUNT(DISTINCT employee_id) FROM EmployeeDWH";
        int count_1 = jdbcTemplate.queryForObject(sql_1, Integer.class);
        System.out.println("Total employee completed training each month -> "+count_1);
        
        String sql_2 = "SELECT COUNT(*) FROM EmployeeDWH GROUP BY MONTH(training_date)";
        int count_2 = jdbcTemplate.queryForObject(sql_2, Integer.class);
        System.out.println("Total training each month -> "+count_2);
		
	}
}
