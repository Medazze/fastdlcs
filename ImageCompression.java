import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.ImageWriteParam;
import javax.imageio.IIOImage;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class ImageCompression {
    
    public static class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
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
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;
            this.conf = context.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                byte[] contents = new byte[(int) fileSplit.getLength()];
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = null;
                try {
                    in = fs.open(file);
                    IOUtils.readFully(in, contents, 0, contents.length);
                    value.set(contents, 0, contents.length);
                } finally {
                    IOUtils.closeStream(in);
                }
                key.set(file.getName());
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
        public float getProgress() throws IOException { return processed ? 1.0f : 0.0f; }

        @Override
        public void close() throws IOException { }
    }

    public static class ImageOutputFormat extends FileOutputFormat<Text, BytesWritable> {
        @Override
        public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext context) 
                throws IOException, InterruptedException {
            return new ImageRecordWriter(context);
        }
    }

    public static class ImageRecordWriter extends RecordWriter<Text, BytesWritable> {
        private final FileSystem fs;
        private final Path outputDir;

        public ImageRecordWriter(TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path outputPath = FileOutputFormat.getOutputPath(context);
            this.fs = outputPath.getFileSystem(conf);
            this.outputDir = outputPath;
        }

        @Override
        public void write(Text key, BytesWritable value) throws IOException {
            String filename = key.toString();
            Path imagePath = new Path(outputDir, filename);
            
            try (FSDataOutputStream out = fs.create(imagePath)) {
                out.write(value.getBytes(), 0, value.getLength());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            // No additional cleanup needed
        }
    }

    public static class ImageCompressionMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
        private static final Log LOG = LogFactory.getLog(ImageCompressionMapper.class);
        
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            byte[] imageBytes = value.getBytes();
            ByteArrayInputStream bais = new ByteArrayInputStream(imageBytes);
            
            try {
                // Detect image format
                ImageInputStream iis = ImageIO.createImageInputStream(bais);
                Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
                
                if (!readers.hasNext()) {
                    LOG.error("No image readers found for: " + key.toString());
                    return;
                }
                
                // Reset stream
                bais.reset();
                BufferedImage image = ImageIO.read(bais);
                
                if (image == null) {
                    LOG.error("Failed to read image: " + key.toString());
                    return;
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                // Convert to JPEG with quality settings
                Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
                ImageWriter writer = writers.next();
                ImageWriteParam param = writer.getDefaultWriteParam();
                param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                param.setCompressionQuality(0.7f); // Adjust quality (0.0-1.0)

                ImageOutputStream ios = ImageIO.createImageOutputStream(baos);
                writer.setOutput(ios);
                writer.write(null, new IIOImage(image, null, null), param);
                writer.dispose();
                ios.close();

                byte[] compressedImageBytes = baos.toByteArray();
                baos.close();

                // Add file extension to output
                String outputKey = key.toString() + ".jpg";
                context.write(new Text(outputKey), new BytesWritable(compressedImageBytes));
                LOG.info("Successfully compressed image: " + outputKey);
            } catch (Exception e) {
                LOG.error("Error processing image: " + key.toString(), e);
            } finally {
                bais.close();
            }
        }
    }

    public static class ImageCompressionReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            for (BytesWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Image Compression");

        job.setJarByClass(ImageCompression.class);
        job.setMapperClass(ImageCompressionMapper.class);
        job.setReducerClass(ImageCompressionReducer.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(ImageOutputFormat.class);  // Set custom output format

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
