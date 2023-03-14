package com.test;

import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.test.service.DataQueryTask;
import com.test.service.ETLDWHAnalyticsTask;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MyApplicationTest {

    @Autowired
    private DataQueryTask dataQueryTask;
    
    @Autowired
    private ETLDWHAnalyticsTask etldwhAnalyticsTask;

    @Test
    public void testJava() {
    	
    	dataQueryTask.createEmployeeTable();
    	dataQueryTask.insertEmployeeData();
    	dataQueryTask.createPositionHistoryTable();
    	dataQueryTask.insertPositionHistoryData();
    	dataQueryTask.getAllEmployeeWithCurrentPos();
    	
    	List<HashMap<String, Object>> employeeList = etldwhAnalyticsTask.getAzureEmployeeData();
    	List<HashMap<String, Object>> trainingHistoryList = etldwhAnalyticsTask.getTrainingHistoryData();
    	etldwhAnalyticsTask.insertDataToDWH(employeeList, trainingHistoryList);
    	etldwhAnalyticsTask.reportTrainingHistoryData();
    	etldwhAnalyticsTask.dashboardTraining();
    }
    
//    @Test
//    public void testEndpoint() {
//        ResponseEntity<String> response = restTemplate.getForEntity("/my-endpoint", String.class);
//        assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
//    }
}
