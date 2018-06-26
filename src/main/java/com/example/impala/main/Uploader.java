package com.example.impala.main;

import com.example.impala.AbstractDataMaker;
import com.example.impala.csv.CsvDataMaker;
import com.example.impala.parquet.ParquetDataMaker;

public class Uploader {

//    private AbstractDataMaker dataMaker = new CsvDataMaker();
    private AbstractDataMaker dataMaker = new ParquetDataMaker();

    private void writeFile() throws Exception {
        dataMaker.writeData();
    }

    public static void main(String[] args) {
        try {
            Uploader uploader = new Uploader();

            //创建文件
            uploader.writeFile();

            //上传文件
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
