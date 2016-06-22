package com.bigdata.bigdog.spark;

import com.bigdata.bigdog.spark.bigdog.MyTestLogic;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class MyTest{
    private static final Logger logger = LoggerFactory.getLogger(MyTest.class);

    private MyTestLogic testLogic;
    @BeforeClass
    public static void setup(){
        
    }
    
    private void finalTask(){
        if(testLogic != null){
          testLogic.testMe();
        }
    }

      @Test
    public void SomeThingTest() {
        testLogic = new WordCount(); 
        finalTask();
    }
    
    
}