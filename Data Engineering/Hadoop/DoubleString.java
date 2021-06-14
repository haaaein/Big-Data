import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DoubleString implements WritableComparable 
{
	String joinKey = new String();
	String tableName = new String();
	
	public DoubleString() {}
	public DoubleString( String _joinKey, String _tableName)
	{
		joinKey = _joinKey;
		tableName = _tableName;
	}
	
	public void readFields(DataInput in) throws IOException
	{
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}
	
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}
	
	public int compareTo(Object o1)
	{
		DoubleString o = (DoubleString) o1;
		
		int ret = joinKey.compareTo(o.joinKey);
		if (ret != 0)
			return ret;
		return -1 * tableName.compareTo( o.tableName );
	}
	
	public String toString() { return joinKey + " " + tableName; }
}
