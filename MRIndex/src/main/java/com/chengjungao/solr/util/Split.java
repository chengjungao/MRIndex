package com.chengjungao.solr.util;

public class Split {
	/**
	     * Split a string to array by the specify separator
	     * If the string ends of the separator, the last element will be missed in Java String.Split().
	     * So the method to keep the last blank element.
	     * @param value
	     * @return
	     */
	    public static String[] split(String value, String separator){
	        
	        String[] rtn=null;
	        if(value != null) {
	            boolean endBlank = false;
	            if(value.endsWith(separator)){
	                value += " ";
	                endBlank = true;
	            }
	            separator = escapeExprSpecialWord(separator);
	            rtn= value.split(separator);
	            
	            if(endBlank){
	                rtn[rtn.length-1] ="";
	            }
	        }
	        return rtn;
	    }
	    
	    /**
	     * Escape the Special word.
	     * @param keyword
	     * @return
	     */
	    public static String escapeExprSpecialWord(String keyword) {  
	        if (keyword != null && !keyword.isEmpty()) {  
	            String[] fbsArr = { "\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|" };  
	            for (String key : fbsArr) {  
	                if (keyword.contains(key)) {  
	                    keyword = keyword.replace(key, "\\" + key);  
	                }  
	            }  
	        }  
	        return keyword;  
	    }  
}

