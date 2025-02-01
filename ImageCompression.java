import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
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

    public static class ImageCompressMapper 
            extends Mapper<Object, BytesWritable, Text, BytesWritable> {

        public void map(Object key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            byte[] imageBytes = value.getBytes();
            Text fileName = new Text(key.toString());
            context.write(fileName, new BytesWritable(imageBytes));
        }
    }

    public static class ImageCompressReducer 
            extends Reducer<Text, BytesWritable, Text, BytesWritable> {

        public void reduce(Text key, Iterable<BytesWritable> values, Context context)
                throws IOException, InterruptedException {
            
            for (BytesWritable val : values) {
                byte[] imageBytes = val.getBytes();
                byte[] compressedBytes = compressImage(imageBytes, 0.5f);
                context.write(key, new BytesWritable(compressedBytes));
            }
        }

        private byte[] compressImage(byte[] imageData, float quality) throws IOException {
            ByteArrayInputStream bis = new ByteArrayInputStream(imageData);
            BufferedImage image = ImageIO.read(bis);

            ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
            ImageWriteParam param = writer.getDefaultWriteParam();
            
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            param.setCompressionQuality(quality);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ImageOutputStream ios = ImageIO.createImageOutputStream(bos);
            writer.setOutput(ios);
            writer.write(null, new IIOImage(image, null, null), param);
            
            writer.dispose();
            ios.close();
            bis.close();
            
            return bos.toByteArray();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "image compression");
        
        job.setJarByClass(ImageCompression.class);
        job.setMapperClass(ImageCompressMapper.class);
        job.setReducerClass(ImageCompressReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
