package lab;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }
    public TextArrayWritable(String [] values) {
        super(Text.class);
        Text[] writableValues = new Text[values.length];
        for (int i = 0; i < values.length; i++) {
            writableValues[i] = new Text(values[i]);
        }
        set(writableValues);
    }
}