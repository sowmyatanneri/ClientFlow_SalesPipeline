/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.google.gson.annotations.Expose;
import faasinspector.fiResponse;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author wlloyd
 */
public class Response extends fiResponse {
    
    
    private LinkedList<Map<String, Object>> salesRecords;
    public  LinkedList<Map<String, Object>> getSalesRecords(){
        return salesRecords;
    }
    
    public void setSalesRecords(LinkedList<Map<String, Object>> salesRecords){
        this.salesRecords = salesRecords;
    }
    
    private String query;
    public String getQuery(){
        return query;
    }

    public void setQuery(String query){
        this.query = query;
    }

    
    private int count;
    public int getCount(){
        return count;
    }
    
    public void setCount(int count){
        this.count = count;
    }
    
    private String message;
    public String getMessage()
    {
        return message;
    }
    public void setMessage(String message)
    {
        this.message = message;
    }
    
    @Override
    public String toString()
    {
        return message;
    }

}
