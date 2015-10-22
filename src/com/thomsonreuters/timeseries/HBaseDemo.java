package com.thomsonreuters.timeseries;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.LogManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Scanner;


public class HBaseDemo {
	public static void main(String[] args) {
	    PropertyConfigurator.configure("log4j.properties");
	    Logger logger = LogManager.getRootLogger();     
	    
	    if (args.length != 1) {
	      System.out.println("usage: <HBase Resource list>");
	      System.exit(0);      
	    }

	    System.out.println("Welcome to HBase demo");
	    Configuration conf = HBaseConfiguration.create();
	    String[] resources = args[0].split(";");
	    for(String res: resources)
	    	conf.addResource(new Path(res));
	    
	    Scanner scan = new Scanner(System.in);
	    prompt();
	    String text = scan.nextLine();
	    while(text.toLowerCase().compareTo("exit") != 0) {
	      // process command
	      String[] cmd = text.split(" ");
	      switch(cmd[0].toLowerCase()) {
	        case "list":
	        	HBaseUtil.listTable(conf);
	        	break;
	        case "create":
	        	if(cmd.length != 3) {
	        		System.out.println("create <tablename> <column family>");
	        	} else {
	        		String tablename = cmd[1];
	        		String family = cmd[2];
	        		HBaseUtil.createTable(conf, tablename, family);
	        	}
	        	break;
	        case "disable":
	        	if(cmd.length != 2) {
	        		System.out.println("disable <tablename>");
	        	} else {
	        		String tablename = cmd[1];
	        		HBaseUtil.disableTable(conf, tablename);
	        	}
	        	break;	        	
	        case "enable":
	        	if(cmd.length != 2) {
	        		System.out.println("enable <tablename>");
	        	} else {
	        		String tablename = cmd[1];
	        		HBaseUtil.enableTable(conf, tablename);
	        	}
	        	break;	        	
	        case "drop":
	        	if(cmd.length != 2) {
	        		System.out.println("drop <tablename>");
	        	} else {
	        		String tablename = cmd[1];
	        		HBaseUtil.deleteTable(conf, tablename);
	        	}
	        	break;	   
	        case "modify":
	        	if(cmd.length != 3) {
	        		System.out.println("modify <tablename> <column family>");
	        	} else {
	        		String tablename = cmd[1];
	        		String family = cmd[2];
	        		HBaseUtil.modifyTable(conf, tablename, family);
	        	}
	        	break;	   
	        case "count":
	        	if(cmd.length != 2) {
	        		System.out.println("count <tablename>");
	        	} else {
	        		String tablename = cmd[1];
	        		HBaseUtil.countTable(conf, tablename);
	        	}
	        	break;	   
	        case "scan":
	        	if(cmd.length < 2 || cmd.length > 5) {
	        		System.out.println("scan <tablename> [family] [startrow] [endrow]");
	        	} else {
	        		String tablename = cmd[1];
	        		String family = null;
	        		String start = null;
	        		String end = null;
	        		if(cmd.length == 3)
	        			family = cmd[2];
	        		else if(cmd.length == 4) {
	        			family = cmd[2];
	        			start = cmd[3];
	        		} else if(cmd.length == 5) {
	        			family = cmd[2];
	        			start = cmd[3];
	        			end = cmd[4];
	        		}
	        			
	        		HBaseUtil.scanTable(conf, tablename, family, start, end);
	        	}
	        	break;	   	        	
	        	
	        case "get":
	        	if(cmd.length != 3 && cmd.length != 4) {
	        		System.out.println("get <tablename> <row> [family]");
	        	} else {
	        		String tablename = cmd[1];
	        		String row = cmd[2];
	        		String family = null;
	        		if(cmd.length == 4)
	        			family = cmd[3];
	        		HBaseUtil.getData(conf, tablename, row, family);
	        	}
	        	break;	   	        	
	        case "put":
	        	if(cmd.length != 5) {
	        		System.out.println("put <tablename> <row> <family> <value>");
	        	} else {
	        		String tablename = cmd[1];
	        		String row = cmd[2];
	        		String family = cmd[3];
	        		String value = cmd[4];
	        		HBaseUtil.putData(conf, tablename, row, family, value);
	        	}
	        	break;	   	        	
	        case "delete":
	        	if(cmd.length != 3 && cmd.length != 4) {
	        		System.out.println("delete <tablename> <row> [family]");
	        	} else {
	        		String tablename = cmd[1];
	        		String row = cmd[2];
	        		String family = null;
	        		if(cmd.length == 4)
	        			family = cmd[3];
	        		HBaseUtil.deleteData(conf, tablename, row, family);
	        	}
	        	break;	   	        	
	        default:
	        	if(text.length() == 0)
	        		break;
	        	System.out.println("unknown command '" + cmd[0] + "'");
	      }
	      
	      
	      prompt();
	      text = scan.nextLine();
	    }
	    
	    
	    logger.info("exit");
	  }
	  
	  static void prompt() {
		  System.out.print(">");
	  }
}
