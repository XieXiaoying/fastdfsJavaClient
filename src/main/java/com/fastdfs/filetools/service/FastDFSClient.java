package com.fastdfs.filetools.service;

import com.alibaba.fastjson.JSONObject;
import com.fastdfs.filetools.entity.FastDFSFile;
import org.csource.common.MyException;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.client.ResourceAccessException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

public class FastDFSClient {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(FastDFSClient.class);

    static {
        try {
            String filePath = new ClassPathResource("fdfs_client.conf").getPath();//.getFile().getAbsolutePath();//.getPath();

            ClientGlobal.init(filePath);
        } catch (Exception e) {
            logger.error("FastDFS Client Init Fail!",e);
        }
    }


    public static JSONObject upload2(String groupName, FastDFSFile file) throws ConnectException {
        Map<String, Object> result = new HashMap<String, Object>();
        logger.info("File Name: " + file.getName() + " File Length:" + file.getContent().length);

        long startTime = System.currentTimeMillis();
        String[] uploadResults = null;
        StorageClient storageClient=null;
        try {
            storageClient = getTrackerClient();
            //System.out.println(storageClient);
            uploadResults = storageClient.upload_file(groupName, file.getContent(), file.getExt(), null);
           // System.out.println(uploadResults.length);
        } catch (ConnectException e){
            throw e;
        } catch (IOException e) {
            if (e.getCause() instanceof ConnectException){
                throw new ConnectException("error ip or port");
            }
        } catch (Exception e) {
            logger.error("Non IO Exception when uploadind the file:" + file.getName(), e);
        }
        logger.info("upload_file time used:" + (System.currentTimeMillis() - startTime) + " ms");

        if (uploadResults == null && storageClient!=null) {
            logger.error("upload file fail, error code:" + storageClient.getErrorCode());
        } else {
            groupName = uploadResults[0];
            String remoteFileName = uploadResults[1];

            logger.info("upload file successfully!!!" + "group_name:" + groupName + ", remoteFileName:" + " " + remoteFileName);
            FileInfo fileInfo = getFile(groupName, remoteFileName);
            result.put("group name", groupName);
            result.put("file id", remoteFileName);
            result.put("file name", file.getName());
            result.put("md5", file.getMd5());

            result.put("server",fileInfo.getSourceIpAddr());
            result.put("file size", fileInfo.getFileSize());
            result.put("upload time", fileInfo.getCreateTimestamp());
        }

        JSONObject json = new JSONObject(result);
//        String res = JSON.toJSONString(result);
        return json;
    }

    public static String[] upload(FastDFSFile file) throws ResourceAccessException, ConnectException {
        logger.info("File Name: " + file.getName() + "File Length:" + file.getContent().length);

        NameValuePair[] meta_list = new NameValuePair[1];

        long startTime = System.currentTimeMillis();
        String[] uploadResults = null;
        StorageClient storageClient=null;
        try {
            storageClient = getTrackerClient();
            uploadResults = storageClient.upload_file(file.getContent(), file.getExt(), null);
        } catch (ResourceAccessException e){
            throw e;
        } catch (IOException e) {
            if (e.getCause() instanceof ConnectException){
                throw new ConnectException("error ip or port");
            }
            logger.error("IO Exception when uploadind the file:" + file.getName(), e);
        } catch (Exception e) {
            logger.error("Non IO Exception when uploadind the file:" + file.getName(), e);
        }
        logger.info("upload_file time used:" + (System.currentTimeMillis() - startTime) + " ms");

        if (uploadResults == null && storageClient!=null) {
            logger.error("upload file fail, error code:" + storageClient.getErrorCode());
        }
        String groupName = uploadResults[0];
        String remoteFileName = uploadResults[1];

        logger.info("upload file successfully!!!" + "group_name:" + groupName + ", remoteFileName:" + " " + remoteFileName);
        logger.info(getFile(groupName, remoteFileName).toString());
        return uploadResults;
    }

    public static FileInfo getFile(String groupName, String remoteFileName) throws ResourceAccessException {
        try {
            StorageClient storageClient = getTrackerClient();
            return storageClient.get_file_info(groupName, remoteFileName);
        } catch (ResourceAccessException e){
            throw e;
        } catch (IOException e) {
            logger.error("IO Exception: Get File from Fast DFS failed", e);
        } catch (Exception e) {
            logger.error("Non IO Exception: Get File from Fast DFS failed", e);
        }
        return null;
    }
    public static String getFilePath(String groupName, String remoteFileName){
        try {
            StorageClient storageClient = getTrackerClient();
            return "http://" + storageClient.getTrackerServer().getInetSocketAddress().getHostString() + ":" + ClientGlobal.getG_tracker_http_port() + "/" + groupName + "/" + remoteFileName;
        } catch (Exception e) {
            logger.error("Non IO Exception: Get File from Fast DFS failed", e);
        }
        return null;
    }

    public static byte[] downFile(String groupName, String remoteFileName) {
        try {
            StorageClient storageClient = getTrackerClient();
            byte[] fileByte = storageClient.download_file(groupName, remoteFileName);
//            System.out.println(fileByte);
//            if(fileByte == null) return null;
            //InputStream ins = new ByteArrayInputStream(fileByte);
            return fileByte;
        } catch (IOException e) {
            logger.error("IO Exception: Get File from Fast DFS failed", e);
        } catch (Exception e) {
            logger.error("Non IO Exception: Get File from Fast DFS failed", e);
        }
        return null;
    }

    public static int deleteFile(String groupName, String remoteFileName)
            throws Exception {
        StorageClient storageClient = getTrackerClient();
        int i = storageClient.delete_file(groupName, remoteFileName);
        logger.info("delete file successfully!!!" + i);
        return i;
    }

    public static StorageServer[] getStoreStorages(String groupName)
            throws IOException, MyException {
        TrackerClient trackerClient = new TrackerClient();
        TrackerServer trackerServer = trackerClient.getTrackerServer();//.getConnection();//
        return trackerClient.getStoreStorages(trackerServer, groupName);
    }

    public static ServerInfo[] getFetchStorages(String groupName,
                                                String remoteFileName) throws IOException, MyException {
        TrackerClient trackerClient = new TrackerClient();
        TrackerServer trackerServer = trackerClient.getTrackerServer();//.getConnection();//getTrackerServer();
        return trackerClient.getFetchStorages(trackerServer, groupName, remoteFileName);
    }

    public static String getTrackerUrl() throws IOException {
        return "http://"+getTrackerServer().getInetSocketAddress().getHostString()+":"+ClientGlobal.getG_tracker_http_port()+"/";
    }

    private static StorageClient getTrackerClient() throws IOException {
        TrackerServer trackerServer = getTrackerServer();
        StorageClient storageClient = new StorageClient(trackerServer, null);
        return  storageClient;
    }

    private static TrackerServer getTrackerServer() throws IOException {
        TrackerClient trackerClient = new TrackerClient();
        TrackerServer trackerServer = trackerClient.getTrackerServer();//.getConnection();//.getTrackerServer();
        return  trackerServer;
    }
}
