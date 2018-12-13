/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import java.util.LinkedList;

/**
 *
 * @author Rahul Deshpande
 */
public class QueryInfo {
    
    public LinkedList<AggregateInfo> aggregateInfo;
    
    public LinkedList<AggregateInfo> getAggregateInfo(){
        return this.aggregateInfo;
    }
    
    public void setAggregateInfo(LinkedList<AggregateInfo> aggregateInfo){
        this.aggregateInfo = aggregateInfo;
    }
    
}
