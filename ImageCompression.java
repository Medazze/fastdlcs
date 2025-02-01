import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import javax.imageio.ImageIO;
import javax.naming.Context;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

public class ImageCompression {
    public static class PngToJpegMapper
            extends Mapper<Text, BytesWritable, Text, BytesWritable> {

        public void map(Text key, BytesWritable value, Context context)
                throws InterruptedException {
            try {
                int originalSize = value.getLength();
                System.out.println("Original image size: " + originalSize + " bytes");

                ByteArrayInputStream bais = new ByteArrayInputStream(value.getBytes());
                BufferedImage pngImage = ImageIO.read(bais);
                if (pngImage == null) {
                    System.err.println("Failed to read image: " + key.toString());
                    return;
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ImageIO.write(pngImage, "jpeg", baos);
                int compressedSize = baos.size();
                System.out.println("Compressed image size: " + compressedSize + " bytes");

                context.write(key, new BytesWritable(baos.toByteArray()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Pass-through reducer
    public static class ImageCompressionReducer
            extends Reducer<Text, BytesWritable, Text, BytesWritable> {
        public void reduce(Text key, Iterable<BytesWritable> values, Context context)
                throws IOException, InterruptedException {
            for (BytesWritable val : values) {
                // Write each compressed image
                context.write(key, val);
            }
        }
    }

    public static class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
        protected boolean isSplitable(org.apache.hadoop.fs.FileSystem fs, Path file) {
            return false;
        }
        public RecordReader<Text, BytesWritable> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new WholeFileRecordReader();
        }
        static class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
            private boolean processed = false;
            private Text key = new Text();
            private BytesWritable value = new BytesWritable();
            private FSDataInputStream fis;
            private long fileLength;
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
                Path file = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split).getPath();
                org.apache.hadoop.fs.FileSystem fs = file.getFileSystem(context.getConfiguration());
                fis = fs.open(file);
                fileLength = fs.getFileStatus(file).getLen();
                key.set(file.toString());
            }
            public boolean nextKeyValue() throws IOException {
                if (!processed) {
                    byte[] contents = new byte[(int) fileLength];
                    IOUtils.readFully(fis, contents, 0, contents.length);
                    value.set(contents, 0, contents.length);
                    processed = true;
                    return true;
                }
                return false;
            }
            public Text getCurrentKey() {
                return key;
            }
            public BytesWritable getCurrentValue() {
                return value;
            }
            public float getProgress() {
                return processed ? 1.0f : 0.0f;
            }
            public void close() throws IOException {
                if (fis != null) fis.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PNG to JPEG Compression");
        job.setJarByClass(ImageCompression.class);
        job.setMapperClass(PngToJpegMapper.class);
        job.setReducerClass(ImageCompressionReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class); // Use SequenceFileOutputFormat
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


