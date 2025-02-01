import java.io.*;
import javax.imageio.*;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
                    IOUtils.closeStream(in);
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

    public static class ImageCompressMapper 
            extends Mapper<Text, BytesWritable, Text, BytesWritable> {

        @Override
        public void map(Text key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ImageCompressReducer 
            extends Reducer<Text, BytesWritable, Text, BytesWritable> {

        @Override
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

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
            ImageWriteParam param = writer.getDefaultWriteParam();
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            param.setCompressionQuality(quality);

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
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setMapperClass(ImageCompressMapper.class);
        job.setReducerClass(ImageCompressReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
