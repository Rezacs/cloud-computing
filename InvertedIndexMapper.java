import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text filenameAndOne = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] words = value.toString().split("\\W+"); // split by non-word chars

        for (String w : words) {
            if (w.length() > 0) {
                word.set(w.toLowerCase());
                filenameAndOne.set(filename + ":1");
                context.write(word, filenameAndOne);
            }
        }
    }
}
