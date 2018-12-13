/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

/**
 *
 * @author Rahul Deshpande
 */
public class Transformer {
    
    public static String transformValue(LambdaLogger logger, String columnName, String value){
        logger.log("colmn = " + columnName + " abd value = " + value);
        if("Order Priority".equals(columnName) || "orderpriority".equals(columnName)){
            // A , H L, M
            return value.substring(0,1).toUpperCase();
        }
        return value;
    }
    
    public static String transformColumn(LambdaLogger logger, String columnName){
        if(columnName.equals("grossmargin")){
            return columnName + "* 100";
        }
        return columnName;
    }
    
}
