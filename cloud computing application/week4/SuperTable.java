import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class

      	Configuration	con	=	HBaseConfiguration.create();
//	Instantiating	HbaseAdmin	class
	HBaseAdmin	admin	=	new	HBaseAdmin(con);
//	Instantiating	table	descriptor	class
	HTableDescriptor tableDescriptor =	new
//	Adding	column	families	to	table	descriptor
	tableDescriptor.addFamily(new	HColumnDescriptor("personal"));
	tableDescriptor.addFamily(new	HColumnDescriptor("professional"));
//	Execute	the	table	through	admin
	admin.createTable(tableDescriptor);

      // Instantiating HTable class
        HTable	hTable	= new	HTable(con,	"powers");
      // Repeat these steps as many times as necessary

	Put p1	= new Put(Bytes.toBytes("row1"));
//	adding	values	using	add()	method
//	accepts	column	family	name,	qualifier/row	name ,value
	p1.add(Bytes.toBytes("personal"),
	p1.add(Bytes.toBytes("personal"),
	p1.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),
	p1.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),
//	Saving	the	put	Instance	to	the	HTable.
	hTable.put(p1);
						
				
	Put p2	= new Put(Bytes.toBytes("row2"));
//	adding	values	using	add()	method
//	accepts	column	family	name,	qualifier/row	name ,value
	p2.add(Bytes.toBytes("personal"),
	p2.add(Bytes.toBytes("personal"),
	p2.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),
	p2.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),
//	Saving	the	put	Instance	to	the	HTable.
	hTable.put(p2);
	
        Put p3	= new Put(Bytes.toBytes("row3"));
//	adding	values	using	add()	method
//	accepts	column	family	name,	qualifier/row	name ,value
	p3.add(Bytes.toBytes("personal"),
	p3.add(Bytes.toBytes("personal"),
	p3.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),
	p3.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),
//	Saving	the	put	Instance	to	the	HTable.
	hTable.put(p3);
						
	//	closing	HTable
	hTable.close();	

      // Instantiate the Scan class
      HTable	table	=	new	HTable(con,	"powers");
      Scan scan	=	new	Scan();
      // Scan the required columns
       scan.addColumn(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
      // Get the scan result
       ResultScanner	scanner	=	table.getScanner(scan);
       
       for(Result result = scanner.next(); result != null; result = scanner.next())
	 System.out.println(result);
//closing	the	scanner
	scanner.close();
      // Read values from scan result
      // Print scan result
 
      // Close the scanner
        table.close();
      // Htable closer
   }
}
