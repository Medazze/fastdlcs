import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class ImageCompression {
    public static class PngToJpegMapper
            extends Mapper<Object, BytesWritable, NullWritable, BytesWritable> {

        public void map(Object key, BytesWritable value, Context context)
                throws InterruptedException {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(value.getBytes());
                BufferedImage pngImage = ImageIO.read(bais);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ImageIO.write(pngImage, "jpeg", baos);
                context.write(NullWritable.get(), new BytesWritable(baos.toByteArray()));
            } catch (Exception e) {
                // Handle exceptions
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PNG to JPEG Compression");
        job.setJarByClass(ImageCompression.class);
        job.setMapperClass(PngToJpegMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


