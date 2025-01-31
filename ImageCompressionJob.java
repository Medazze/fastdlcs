import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.imageio.ImageIO;
import javax.imageio.IIOImage;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Iterator;

public class ImageCompressionJob {
    // Main class
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ImageCompressionJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Image Compression");
        
        job.setJarByClass(ImageCompressionJob.class);
        job.setMapperClass(ImageMapper.class);
        job.setReducerClass(ImageReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ImageWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setInputFormatClass(ImageInputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Add ImageInputFormat as inner class
    public static class ImageInputFormat extends FileInputFormat<Text, BufferedImage> {
        @Override
        public RecordReader<Text, BufferedImage> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new ImageRecordReader();
        }

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }
    }

    // Add ImageRecordReader as inner class
    public static class ImageRecordReader extends RecordReader<Text, BufferedImage> {
        private Text key = null;
        private BufferedImage value = null;
        private boolean processed = false;
        private Path filePath;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
            filePath = ((FileSplit) split).getPath();
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!processed) {
                key = new Text(filePath.getName());
                value = ImageIO.read(filePath.toUri().toURL());
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() {
            return key;
        }

        @Override
        public BufferedImage getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() {
            // No resources to close
        }
    }

    // Mapper class updated to handle binary input
    public static class ImageMapper extends Mapper<Text, BufferedImage, Text, ImageWritable> {
        @Override
        public void map(Text key, BufferedImage image, Context context) throws IOException, InterruptedException {
            if (image != null) {
                context.write(key, new ImageWritable(image));
            }
        }
    }

    // Reducer class
    public static class ImageReducer extends Reducer<Text, ImageWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<ImageWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            for (ImageWritable value : values) {
                BufferedImage image = value.getImage();
                
                // Create JPEG writer
                Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
                ImageWriter writer = writers.next();
                
                // Configure compression
                ImageWriteParam param = writer.getDefaultWriteParam();
                param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                param.setCompressionQuality(0.7f); // Adjust compression level (0.0-1.0)
                
                // Write compressed JPEG
                String outputPath = context.getConfiguration().get("mapred.output.dir") + 
                                  "/" + key.toString().replace(".png", ".jpg");
                Path path = new Path(outputPath);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                OutputStream os = fs.create(path);
                
                writer.setOutput(ImageIO.createImageOutputStream(os));
                writer.write(null, new IIOImage(image, null, null), param);
                
                writer.dispose();
                os.close();
                
                context.write(key, new Text(outputPath));
            }
        }
    }

    // Custom Writable class for image data
    public static class ImageWritable implements Writable {
        private BufferedImage image;
        
        public ImageWritable() {}
        
        public ImageWritable(BufferedImage image) {
            this.image = image;
        }
        
        public BufferedImage getImage() {
            return image;
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(image, "png", baos);
            byte[] bytes = baos.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            image = ImageIO.read(bais);
        }
    }
}
