import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.mortbay.util.Scanner;

public class MovieLensHBase {

	public MovieLensHBase() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String movieId;
		String title;
		String genre;
		File f = new File("/Users/karanmankodi/Documents/Coursework/BigData/Workspace/HBaseMovieLens/src/movies.dat");
		java.util.Scanner s = new java.util.Scanner(f);
		int i =1;
		  
		HBaseConfiguration config = new HBaseConfiguration();
		HBaseAdmin admin = new HBaseAdmin(config);
		
		if (!(admin.tableExists("Movies"))) {
		
			HTableDescriptor ht = new HTableDescriptor("Movies");
			ht.addFamily( new HColumnDescriptor("movieid"));
			ht.addFamily( new HColumnDescriptor("title"));
			ht.addFamily( new HColumnDescriptor("genre"));
	
			System.out.println( "connecting" );
			HBaseAdmin hba = new HBaseAdmin( config );
			System.out.println( "Creating Table" );
			hba.createTable( ht );
			System.out.println("Created Table");
		}
		
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

		HTable table = new HTable(conf, "Movies");
		
		

		while (s.hasNextLine()) {
			String[] movies = s.nextLine().split("::");
			movieId = movies[0];
			title = movies[1];
			genre = movies[2];
			
			Put p = new Put(Bytes.toBytes("row"+i));
			p.add(Bytes.toBytes("movieid"), Bytes.toBytes("col1"),Bytes.toBytes(movieId));
			p.add(Bytes.toBytes("title"), Bytes.toBytes("col2"),Bytes.toBytes(title));
			p.add(Bytes.toBytes("genre"), Bytes.toBytes("col3"),Bytes.toBytes(genre));
			table.put(p);
			
			System.out.println("## "+i+" - MovieID = "+movieId+" ; Title = "+title+" ; Genre = "+genre);
			i++;
		}
	
	}

}
