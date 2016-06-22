package com.bigdata.bigdog.spark;

import com.bigdata.bigdog.spark.bigdog.MyTestLogic;
import org.junit.BeforeClass;
import org.junit.Test;
public class MyTest{
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