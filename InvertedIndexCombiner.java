import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> fileCountMap = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String filename = parts[0];
            int count = Integer.parseInt(parts[1]);
            fileCountMap.put(filename, fileCountMap.getOrDefault(filename, 0) + count);
        }

        for (String filename : fileCountMap.keySet()) {
            context.write(key, new Text(filename + ":" + fileCountMap.get(filename)));
        }
    }
}
