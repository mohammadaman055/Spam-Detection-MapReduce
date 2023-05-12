import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

public class Spammapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{

	public void map(LongWritable key, Text value, 
			OutputCollector<Text, Text> output, Reporter r)
					throws IOException {

		String line = value.toString();
		String[] items = line.split(",");
		
		String company = items[0];
		String review = items[4];
		
		output.collect(new Text(company), new Text(review));
		
	}
}
