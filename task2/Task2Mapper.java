/////////Map only job to filter out NA records
package mapreduce.demo.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class Task2Mapper extends Mapper<LongWritable,Text,NullWritable,Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String lineArray[]=value.toString().split("|");
		String company= lineArray[0];
		String model= lineArray[1];
		if (company.equals("NA"))
		{
			context.write(NullWritable.get(), value);
		}
		
		if (model.equals("NA"))
		{
			context.write(NullWritable.get(), value);
		}
		
	}

}
