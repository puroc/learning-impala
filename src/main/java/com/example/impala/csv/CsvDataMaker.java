package com.example.impala.csv;

import com.example.impala.AbstractDataMaker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class CsvDataMaker extends AbstractDataMaker {

    public static final String FOLDER = "/Users/puroc/git/learning-impala/target/csv";
    public static final String FILE_DEVICE = "/Users/puroc/git/learning-impala/target/csv/device.csv";
    public static final String FILE_METRICS = "/Users/puroc/git/learning-impala/target/csv/metrics.csv";
    public static final int DEVICE_TOTAL_NUM = 1000;
    public static final int METRICS_TOTAL_NUM = 9000000;
    private static final List<String> dateList =  new ArrayList<String>();

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


    public void writeData() throws Exception {
        //创建文件夹
        this.createFolder(FOLDER);
        //创建设备文件，deviceId,deviceName,orgId
        writeDeviceData();
        //创建采集指标文件，deviceId,reading,timestamp
        writeMetricsData();
        System.out.println("数据写入完毕");
    }

    private void writeMetricsData() throws IOException, ParseException {
        File file = createFile(FILE_METRICS);
        FileOutputStream fos = new FileOutputStream(file);
        Random random = new Random();
        for (String date : dateList) {
            for (int i = 0; i < METRICS_TOTAL_NUM; i++) {
                String deviceId = "device" + random.nextInt(DEVICE_TOTAL_NUM);
                int reading = random.nextInt(1000);
                String end ="\r\n";
                String data = deviceId + "," + reading + "," + date+end;
                fos.write(data.getBytes());
            }
        }

        fos.close();
    }

    private void writeDeviceData() throws IOException {
        File file = createFile(FILE_DEVICE);
        FileOutputStream fos = new FileOutputStream(file);
        Random random = new Random();
        for (int i = 0; i < DEVICE_TOTAL_NUM; i++) {
            String deviceId = "device" + i;
            String deviceName = "水表" + i;
            String orgId = random.nextInt(5) + "";
            String end ="\r\n";
            String data = deviceId + "," + deviceName + "," + orgId+end;
            fos.write(data.getBytes());
        }
        fos.close();
    }

    private File createFile(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            boolean result = file.createNewFile();
            if (!result) {
                throw new RuntimeException("文件创建失败，" + FILE_DEVICE);
            }
        }
        return file;
    }
}
