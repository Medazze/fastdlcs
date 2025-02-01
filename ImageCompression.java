package imageC;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImageCompression {

    public static class ImageCompressionMapper 
            extends Mapper<Object, BytesWritable, Text, BytesWritable> {
        
        private final float compressionQuality = 0.5f;
        
        public void map(Object key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            
            // Convert byte array to BufferedImage
            ByteArrayInputStream bais = new ByteArrayInputStream(value.getBytes());
            BufferedImage image = ImageIO.read(bais);
            
            // Compress image
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(image, "jpg", baos);
            byte[] compressedBytes = baos.toByteArray();
            
            // Write compressed bytes
            context.write(new Text("image"), new BytesWritable(compressedBytes));
        }
    }

    public static class ImageCompressionReducer 
            extends Reducer<Text, BytesWritable, Text, BytesWritable> {
        
        public void reduce(Text key, Iterable<BytesWritable> values, Context context)
                throws IOException, InterruptedException {
            
            // Take the first compressed chunk as final result
            for (BytesWritable val : values) {
                context.write(key, val);
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "image compression");
        
        job.setJarByClass(ImageCompression.class);
        job.setMapperClass(ImageCompressionMapper.class);
        job.setReducerClass(ImageCompressionReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
