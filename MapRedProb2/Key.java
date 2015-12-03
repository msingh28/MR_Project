import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class Key implements WritableComparable<Key> {
	Integer year;
	Double count;

	@Override
	public void readFields(DataInput arg0) throws IOException {
		year = WritableUtils.readVInt(arg0);
		count = arg0.readDouble();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeVInt(arg0, year);
		arg0.writeDouble(count);

	}

	@Override
	public int compareTo(Key arg0) {
		int result = year.compareTo(arg0.year);
		if (0 == result) {
			result = count.compareTo(arg0.count);
		}
		return result;
	}
}
