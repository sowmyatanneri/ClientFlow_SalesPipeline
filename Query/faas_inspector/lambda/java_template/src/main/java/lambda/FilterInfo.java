/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

/**
 *
 * @author Rahul Deshpande
 */
public class FilterInfo {
    
    private String columnname;
    
    public String getColumnname(){
        return this.columnname;
    }
    public void setColumnname(String columnname){
        this.columnname = columnname;
    }
    
    String columnvalue;
    public String getColumnvalue(){
        return this.columnvalue;
    }
    
    public void setColumnvalue(String columnvalue){
        this.columnvalue = columnvalue;
    }    
    
    @Override
    public  String toString(){
        return "column = " + this.columnname + " and value = " + this.columnvalue;
    }
    
    public FilterInfo(String columnname, String columnvalue){
        this.columnname = columnname;
        this.columnvalue = columnvalue;
    }
    
    public FilterInfo(){
        
    }
}
