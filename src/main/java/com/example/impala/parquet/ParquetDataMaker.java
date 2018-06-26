package com.example.impala.parquet;

import com.example.impala.AbstractDataMaker;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.example.impala.csv.CsvDataMaker.FILE_METRICS;

public class ParquetDataMaker extends AbstractDataMaker {

    public static final String FOLDER = "/Users/puroc/git/learning-impala/target/parquet";
    public static final String FILE_DEVICE = "/Users/puroc/git/learning-impala/target/parquet/device/device.parq";
    public static final String FILE_METRICS_PREFIX = "/Users/puroc/git/learning-impala/target/parquet/metrics/metrics";
    public static final String FILE_METRICS_SUFFIX = ".parq";
    public static final int DEVICE_TOTAL_NUM = 1000;
    public static final int METRICS_TOTAL_NUM = 9000000;
    private static final List<String> dateList = new ArrayList<String>();

    {
        dateList.add("2017-01-01 18:00:00");
        dateList.add("2017-02-01 18:00:00");
        dateList.add("2017-03-01 18:00:00");
        dateList.add("2017-04-01 18:00:00");
        dateList.add("2017-05-01 18:00:00");
        dateList.add("2017-06-01 18:00:00");

        dateList.add("2018-07-01 18:00:00");
        dateList.add("2018-08-01 18:00:00");
        dateList.add("2018-09-01 18:00:00");
        dateList.add("2018-10-01 18:00:00");
        dateList.add("2018-11-01 18:00:00");
        dateList.add("2018-12-01 18:00:00");
    }

    private static final String DEVICE_SCHEMA = "message schema {"
            + "optional binary deviceId;"
            + "optional binary deviceName;"
            + "optional binary orgId;"
            + "}";

    private static final String METRICS_SCHEMA = "message schema {"
            + "optional binary deviceId;"
            + "optional int64 reading;"
            + "optional binary time;"
            + "}";

    private MessageType deviceSchema = MessageTypeParser.parseMessageType(DEVICE_SCHEMA);

    private MessageType metricsSchema = MessageTypeParser.parseMessageType(METRICS_SCHEMA);


    public void writeData() throws Exception {
        //创建文件夹
        this.createFolder(FOLDER);
        //创建设备文件
        writeDeviceData();
        //创建采集指标文件
        writeMetricsData();

    }

    class Task implements Runnable {

        private String date;
        private String path;

        public Task(String date, String path) {
            this.date = date;
            this.path = path;
        }

        public void run() {
            ParquetWriter<Group> writer = null;
            try {
                Path path = new Path(this.path);
                ExampleParquetWriter.Builder builder = ExampleParquetWriter
                        .builder(path).withWriteMode(ParquetFileWriter.Mode.CREATE)
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        //.withConf(configuration)
                        .withType(metricsSchema);

                Random random = new Random();
                writer = builder.build();
                SimpleGroupFactory groupFactory = new SimpleGroupFactory(metricsSchema);
                for (int i = 0; i < METRICS_TOTAL_NUM; i++) {
                    String deviceId = "device" + random.nextInt(DEVICE_TOTAL_NUM);
                    long reading = random.nextInt(1000);
                    writer.write(groupFactory.newGroup()
                            .append("deviceId", deviceId)
                            .append("reading", reading)
                            .append("time", date));
                    if (i % 10000 == 0) {
                        System.out.println(path + "(" + i + ")");
                    }
                }

            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }

    private void writeMetricsData() throws IOException, ParseException, InterruptedException {
        int fileIndex = 0;
        List<Thread> list = new ArrayList<Thread>();
        for (String date : dateList) {
            String path = FILE_METRICS_PREFIX + fileIndex + FILE_METRICS_SUFFIX;
            fileIndex++;
            list.add(new Thread(new Task(date, path)));
        }

        for (Thread thread : list) {
            thread.start();
        }

        for (Thread thread : list) {
            thread.join();
        }

        System.out.println("数据写入完毕");

    }

    private void writeDeviceData() throws IOException {
        Path path = new Path(FILE_DEVICE);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(path).withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                //.withConf(configuration)
                .withType(deviceSchema);

        Random random = new Random();
        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(deviceSchema);
        for (int i = 0; i < DEVICE_TOTAL_NUM; i++) {
            String deviceId = "device" + i;
            String deviceName = "水表" + i;
            String orgId = random.nextInt(5) + "";
            writer.write(groupFactory.newGroup()
                    .append("deviceId", deviceId)
                    .append("deviceName", deviceName)
                    .append("orgId", orgId));
        }
        writer.close();
    }

//    private File createFile(String path) throws IOException {
//        File file = new File(path);
//        if (!file.exists()) {
//            boolean result = file.createNewFile();
//            if (!result) {
//                throw new RuntimeException("文件创建失败，" + FILE_DEVICE);
//            }
//        }
//        return file;
//    }
}
