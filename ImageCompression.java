import java.io.*;
import javax.imageio.*;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  // Add this import
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.IOUtils;

public class ImageCompression {

    public static class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }

        @Override
        public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, 
                TaskAttemptContext context) {
            return new WholeFileRecordReader();
        }
    }

    public static class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
        private FileSplit fileSplit;
        private Configuration conf;
        private boolean processed = false;
        private Text key = new Text();
        private BytesWritable value = new BytesWritable();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
            this.fileSplit = (FileSplit) split;
            this.conf = context.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!processed) {
                byte[] contents = new byte[(int) fileSplit.getLength()];
                Path file = fileSplit.getPath();
                key.set(file.getName());

                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = null;

                try {
                    in = fs.open(file);
                    IOUtils.readFully(in, contents, 0, contents.length);
                    value.set(contents, 0, contents.length);
                } finally {
                    if (in != null) {
                        in.close();  // Changed from IOUtils.closeStream to direct close
                    }
                }
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() { return key; }

        @Override
        public BytesWritable getCurrentValue() { return value; }

        @Override
        public float getProgress() { return processed ? 1.0f : 0.0f; }

        @Override
        public void close() { }
    }

    public static class ImageCompressionMapper
            extends Mapper<Object, BytesWritable, Text, BytesWritable> {

        private Text imageName = new Text();

        public void map(Object key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            // Pass through the image data with its name as the key
            imageName.set("image-chunk");
            context.write(imageName, value);
        }
    }

    public static class ImageCompressionReducer
            extends Reducer<Text, BytesWritable, Text, BytesWritable> {

        public void reduce(Text key, Iterable<BytesWritable> values, Context context)
                throws IOException, InterruptedException {
            for (BytesWritable val : values) {
                // Convert BytesWritable to BufferedImage
                ByteArrayInputStream bis = new ByteArrayInputStream(val.getBytes());
                BufferedImage image = ImageIO.read(bis);
                
                // Compress the image
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
                ImageWriteParam param = writer.getDefaultWriteParam();
                param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                param.setCompressionQuality(0.5f);  // Set compression quality (0.0-1.0)
                
                writer.setOutput(ImageIO.createImageOutputStream(bos));
                writer.write(null, new IIOImage(image, null, null), param);
                
                // Write compressed image
                byte[] compressedImageData = bos.toByteArray();
                context.write(key, new BytesWritable(compressedImageData));
                
                writer.dispose();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "image compression");
        job.setJarByClass(ImageCompression.class);
        
        // Set classes
        job.setMapperClass(ImageCompressionMapper.class);
        job.setReducerClass(ImageCompressionReducer.class);
        
        // Set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        
        // Set input/output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
