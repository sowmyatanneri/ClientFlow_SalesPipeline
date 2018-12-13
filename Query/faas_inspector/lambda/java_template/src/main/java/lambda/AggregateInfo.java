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
public class AggregateInfo {
 
    private String columnname;
        
    public String getColumnname(){
        return columnname;
    }
    public void setColumnname(String columnname){
        this.columnname = columnname;
    }
    
    private String type;
    
    public String getType(){
        return type;
    }
    public void setType(String type){
        this.type = type;
    }
        
    private String units;

    public String getUnits(){
        return units;
    }
    public void setUnits(String units){
        this.units = units;
    }
    
    public String toString(){
        return "col = " + columnname + " and type = " +  type + " and units = "  + units;
    }
    
}
