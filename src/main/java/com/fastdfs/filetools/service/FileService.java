package com.fastdfs.filetools.service;

import com.alibaba.fastjson.JSONObject;
import com.fastdfs.filetools.entity.FastDFSFile;
import com.fastdfs.filetools.utils.FormatUtils;
import org.csource.fastdfs.FileInfo;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.ConnectException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.util.HashMap;
import java.util.Map;
@Service
public class FileService {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(FastDFSClient.class);

    public JSONObject uploadBlob(String groupName, String fileName, Blob data) throws NoSuchAlgorithmException, ConnectException {
        byte[] file_buff = FormatUtils.blobToBytes(data);
        return uploadByteFile(groupName, fileName, file_buff);
    }
    public JSONObject uploadByteFile(String groupName, String fileName, byte[] file_buff) throws NoSuchAlgorithmException, ConnectException {
        JSONObject res = null;
        String ext = fileName.substring(fileName.lastIndexOf(".") + 1);
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] digest = md5.digest(file_buff);
        String hashString = new BigInteger(1, digest).toString(16);
        FastDFSFile file = new FastDFSFile(fileName, file_buff, ext, hashString);
        try {
            res = FastDFSClient.upload2(groupName, file);  //upload to fastdfs
        } catch (ConnectException e){
            throw e;
        }
        if (res==null) {
            logger.error("upload file failed,please upload again!");
        }

        return res;
    }
    public JSONObject getFileInfo(String groupName, String remoteFileName){

        Map<String, Object> result = new HashMap<String, Object>();
        FileInfo fileInfo = FastDFSClient.getFile(groupName, remoteFileName);
        result.put("group name", groupName);
        result.put("file id", remoteFileName);
        if(fileInfo == null){
            result.put("status", 0);
            result.put("message", "get file info failed, no this file!");
            logger.error("get file info failed, no this file!");
            JSONObject json = new JSONObject(result);
            return json;
        }
        result.put("status", 1);
        result.put("message", "get file info succeed!");
        result.put("file size", fileInfo.getFileSize());
        result.put("upload time", fileInfo.getCreateTimestamp());
        result.put("download path",getFilePath(groupName, remoteFileName));
        logger.info("get file info succeed!");
        JSONObject json = new JSONObject(result);
        return json;
    }
    public JSONObject deleteFile(String groupName, String remoteFileName) {
        JSONObject json = new JSONObject();
        try{
            int res = FastDFSClient.deleteFile(groupName, remoteFileName);
            if(res < 0){
                logger.error("delete file failed!");

            }
            json.put("status", 1);
            json.put("message", "success");
            logger.info("delete file succeed!");
            return json;
        } catch (Exception ex){
            json.put("status", 0);
            json.put("message", ex.getCause());
            return json;
        }

    }
    public byte[] downloadFile(String groupName, String remoteFileName) {

//        Map<String, Object> result = new HashMap<String, Object>();
        return FastDFSClient.downFile(groupName, remoteFileName);
//        InputStream inputStream = FastDFSClient.downFile(groupName, remoteFileName);
//
//        result.put("fileContent", inputStream);
//        if(inputStream == null){
//            result.put("status", 0);
//            result.put("message", inputStream);
//            logger.error("download file failed, no this file!");
//        } else {
//            result.put("status", 1);
//            result.put("message", inputStream);
//            logger.info("download file info succeed!");
//        }
//
//        JSONObject json = new JSONObject(result);
//        return json;
    }
    public String getFilePath(String groupName, String remoteFileName){
        return FastDFSClient.getFilePath(groupName, remoteFileName);
    }

    public JSONObject saveFile(String groupName, MultipartFile multipartFile) throws IOException, NoSuchAlgorithmException {
        JSONObject fileAbsolutePath;
//        String[] fileAbsolutePath={};
        String fileName=multipartFile.getOriginalFilename();
        String ext = fileName.substring(fileName.lastIndexOf(".") + 1);
        byte[] file_buff = null;
        InputStream inputStream=multipartFile.getInputStream();
        if(inputStream!=null){
            int len1 = inputStream.available();
            file_buff = new byte[len1];
            inputStream.read(file_buff);
        }
        inputStream.close();
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] digest = md5.digest(file_buff);
        String hashString = new BigInteger(1, digest).toString(16);
        FastDFSFile file = new FastDFSFile(fileName, file_buff, ext, hashString);

        JSONObject json = new JSONObject();
        try {
            fileAbsolutePath = FastDFSClient.upload2(groupName, file);  //upload to fastdfs
            json.put("message","upload file failed!");
            if (fileAbsolutePath==null) {
                json.put("message","upload success");
                logger.error("upload file failed,please upload again!");
            }
            fileAbsolutePath.put("server", FastDFSClient.getTrackerUrl());
            fileAbsolutePath.put("status",1);
            return fileAbsolutePath;
        } catch (Exception e) {
            json.put("status",0);
            json.put("message",e.getCause());
            logger.error("upload file Exception!",e);
        }
        return json;
    }
}
