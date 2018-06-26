package com.example.impala;

import java.io.File;

public abstract class AbstractDataMaker {

    protected void createFolder(String path) throws Exception{
        File file = new File(path);
        if (file.exists()) {
            this.removeFolder(path);
        }
        boolean result = file.mkdirs();
        if (!result) {
            throw new RuntimeException("文件夹创建失败，" + path);
        }
    }

    private void removeFolder(String path) throws Exception{
        File folder = new File(path);
        if (folder.exists()) {
            File[] files = folder.listFiles();
            for (File file : files) {
                if(file.isDirectory()){
                    removeFolder(file.getPath());
                }
                if(file.exists()){
                    boolean result = file.delete();
                    System.out.println((result ? "文件删除成功" : "文件删除失败")+","+file.getName());
                }
            }
            boolean result = folder.delete();
            if (result) {
                System.out.println("文件夹已删除" + path);
            }
        }

    }

    abstract public void writeData() throws Exception;


}
