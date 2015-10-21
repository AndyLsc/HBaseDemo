package com.thomsonreuters.timeseries;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
	  static void listTable(Configuration conf) {
		    HBaseAdmin admin;
			try {
				admin = new HBaseAdmin(conf);
			    HTableDescriptor[] desp = admin.listTables();
			    System.out.println("Tables:");
			    for(HTableDescriptor d:desp) {
			    	System.out.println(d.getNameAsString());
			    }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }
	  
	  static void createTable(Configuration conf, String tablename) {
			HBaseAdmin admin;
			try {
				admin = new HBaseAdmin(conf);
				if(admin.tableExists(tablename)) {
					System.out.println("Table '" + tablename + "' exists.");
				} else {
					HTableDescriptor htd = new HTableDescriptor(tablename);
					HColumnDescriptor cf = new HColumnDescriptor("c");
					cf.setTimeToLive(65535);
					htd.addFamily(cf);
				    admin.createTable(htd);
				    System.out.println("Successfully create table '" + tablename + "'");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }
	  
	  static void enableTable(Configuration conf, String tablename) {
			HBaseAdmin admin;
			try {
				admin = new HBaseAdmin(conf);
				if(!admin.tableExists(tablename)) {
					System.out.println("Table '" + tablename + "' does not exist.");
				} else {
				    admin.enableTable(tablename);
				    System.out.println("Successfully enable table '" + tablename + "'");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }
	  
	  static void disableTable(Configuration conf, String tablename) {
			HBaseAdmin admin;
			try {
				admin = new HBaseAdmin(conf);
				if(!admin.tableExists(tablename)) {
					System.out.println("Table '" + tablename + "' does not exist.");
				} else {
				    admin.disableTable(tablename);
				    System.out.println("Successfully disable table '" + tablename + "'");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

	  static void deleteTable(Configuration conf, String tablename) {
			HBaseAdmin admin;
			try {
				admin = new HBaseAdmin(conf);
				if(!admin.tableExists(tablename)) {
					System.out.println("Table '" + tablename + "' does not exist.");
				} else {
				    admin.deleteTable(tablename);
				    System.out.println("Successfully delete table '" + tablename + "'");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

	  static void modifyTable(Configuration conf, String tablename) {
			HBaseAdmin admin;
			try {
				admin = new HBaseAdmin(conf);
				if(!admin.tableExists(tablename)) {
					System.out.println("Table '" + tablename + "' does not exists.");
				} else {
					HColumnDescriptor cf = new HColumnDescriptor("cc");
					cf.setTimeToLive(65535);
				    admin.addColumn(tablename, cf);
				    System.out.println("Successfully modify table '" + tablename + "'");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

	  static void countTable(Configuration conf, String tablename) {
		  HTable table;
		  try {
			  table = new HTable(conf, tablename.getBytes());
			  Scan scan = new Scan();
			  ResultScanner scanner = table.getScanner(scan);
			  
			  int count = 0;
			  while(scanner.next() != null)
				  count++;
			  
			  scanner.close();
			  table.close();
			  System.out.println(new StringBuffer("count: ").append(count).toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

	  static void scanTable(Configuration conf, String tablename, String cf, String startrow, String endrow) {
		  HTable table;
		  try {
			  table = new HTable(conf, tablename.getBytes());
			  Scan scan = new Scan();
			  if(cf != null)
				  scan.addFamily(cf.getBytes());
			  if(startrow != null)
				  scan.setStartRow(startrow.getBytes());
			  if(endrow != null)
				  scan.setStopRow(endrow.getBytes());
			  ResultScanner scanner = table.getScanner(scan);
			  
			  Result result;
			  while((result = scanner.next()) != null) {
				  Cell[] cells = result.rawCells();
				  for(Cell cell: cells) {
					  String key = new String(cell.getRow());
					  String family = new String(cell.getFamily());
					  String value = new String(cell.getValue());
					  System.out.println(key + "\t" + family + ":" + value);
				  }				  
			  }
			  
			  scanner.close();
			  table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

	  static void putData(Configuration conf, String tablename, String row, String family, String value) {
		  HTable table;
		  try {
			  table = new HTable(conf, tablename.getBytes());
			  Put put = new Put(row.getBytes());
			  put.add(family.getBytes(), null, value.getBytes());
			  table.put(put);
			  System.out.println("Successfully put record.");
			  
			  table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

	  static void getData(Configuration conf, String tablename, String row, String cf) {
		  HTable table;
		  try {
			  table = new HTable(conf, tablename.getBytes());
			  Get get = new Get(row.getBytes());
			  if(cf != null)
				  get.addFamily(cf.getBytes());
			  
			  Result result = table.get(get);
			  Cell[] cells = result.rawCells();
			  for(Cell cell: cells) {
				  String key = new String(cell.getRow());
				  String family = new String(cell.getFamily());
				  String value = new String(cell.getValue());
				  System.out.println(key + "\t" + family + ":" + value);
			  }				  
			  
			  table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

	  static void deleteData(Configuration conf, String tablename, String row, String cf) {
		  HTable table;
		  try {
			  table = new HTable(conf, tablename.getBytes());
			  Delete delete = new Delete(row.getBytes());
			  if(cf != null)
				  delete.addFamily(cf.getBytes());
			  
			  table.delete(delete);
			  System.out.println("Successfully delete record.");
			  
			  table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }

}
