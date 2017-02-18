package com.wwh.es.compare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompareFileName {

    private static final String idFile = "F:\\数据文件\\bdmi4-id\\bdmi4#p1_0.json";

    private static final String fileDir = "F:\\数据文件\\html\\";

    private static final String fileSuffix = ".html.us";

    private static List<String> existIdList = new ArrayList<>(900000);

    private static long total = 0;
    private static long findLost = 0;

    public static void main(String[] args) {

        // 一次性加载全部的ID列表
        BufferedReader idFileRead = null;

        try {
            idFileRead = new BufferedReader(new FileReader(idFile));
            String id;
            while ((id = idFileRead.readLine()) != null) {
                existIdList.add(id);
            }

        } catch (IOException e) {
            e.printStackTrace();
            return;
        } finally {
            if (idFileRead != null)
                try {
                    idFileRead.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }

        File fDirectory = new File(fileDir);

        traversalFiles(fDirectory);

        System.out.println("总共处理：");
        System.out.println(total);
        System.out.println("总共找到丢失的数据：");
        System.out.println(findLost);

    }

    private static void traversalFiles(File fDirectory) {

        if (!fDirectory.isDirectory()) {
            handleFile(fDirectory);
            return;
        }

        File[] flist = fDirectory.listFiles();

        for (File file : flist) {
            if (file.isDirectory()) {
                // 递归
                traversalFiles(file);
            } else {
                handleFile(file);

            }
        }
    }

    private static void handleFile(File file) {
        // 判断后缀名
        String fileName = file.getName();

        if (!fileName.endsWith(fileSuffix)) {
            return;
        }

        String id = fileName.substring(0, (fileName.length() - fileSuffix.length()));

        // 判断ID是否存在
        if (!existIdList.contains(id)) {

            findLost++;
            // 进行重命名
            file.renameTo(new File(file.getParentFile(), fileName + ".lost"));

        }

        total++;

        if (total % 1000 == 0) {
            System.out.println("total handle : " + total);
            System.out.println("find lost : " + findLost);
        }
    }

}
