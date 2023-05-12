import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
public class Spamreducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter r)
					throws IOException {
		int s_count=0;
		int ns_count=0;
		
		String result = "not spam";
		
		while (values.hasNext())
		{
			Text i = values.next();
			if((i.getLength())==4)
			{
				s_count++;
			}
			else
			{
				ns_count++;
			}
		}
		
		if(s_count > ns_count)
		{
			result="spam";
		}
		
			//Write output
		output.collect(key,new Text(result));
	}
}