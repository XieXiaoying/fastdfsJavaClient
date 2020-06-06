package com.fastdfs.filetools.controller;

import com.alibaba.fastjson.JSONObject;
import com.fastdfs.filetools.service.FileService;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.util.Date;
import java.util.HashSet;

@RestController
public class FileController {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(FileController.class);
    private final String groupName = "group2";
    @Autowired
    FileService fileService;

    @PostMapping(value = "/uploadBlobFile")
    public String uploadBlobFile(String fileName, @RequestParam("file") Blob file) {
        JSONObject result = new JSONObject();
        try{
            JSONObject res = fileService.uploadBlob(groupName, fileName, file);
            if(res == null){
                result.put("status", 0);
                result.put("message", "fail");
                return result.toJSONString();
            }
            res.put("status", 1);
            res.put("message", "success");
            return res.toJSONString();
        } catch (ConnectException ex){
            result.put("status", 0);
            result.put("message", ex.getCause());
            return result.toJSONString();
        } catch ( NoSuchAlgorithmException ex){
            result.put("status", 0);
            result.put("message", ex.getCause());
            return result.toJSONString();
        }
    }

    @PostMapping(value = "/uploadByteFile")
    public String uploadByteFile(String fileName, @RequestParam("file") byte[] file) {
        JSONObject result = new JSONObject();
        try{
            JSONObject res = fileService.uploadByteFile(groupName, fileName, file);
            res.put("status", 1);
            res.put("message", "success");
            return res.toJSONString();
        } catch (ConnectException ex){
            result.put("status", 0);
            result.put("message", ex.getCause());
            return result.toJSONString();
        } catch (NoSuchAlgorithmException ex){
            result.put("status", 0);
            result.put("message", ex.getCause());
            return result.toJSONString();
        }
    }

    @PostMapping(value = "/uploadCommonFile")
    public String uploadCommonFile(@RequestParam("file") MultipartFile file) {
        JSONObject result = new JSONObject();
        try{
            JSONObject res = fileService.saveFile(groupName, file);
            if(res == null){
                result.put("status", 0);
                result.put("message", "fail");
                return result.toJSONString();
            }
            res.put("status", 1);
            res.put("message", "success");
            return res.toJSONString();
        } catch (ConnectException ex){
            result.put("status", 0);
            result.put("message", ex.getCause());
            return result.toJSONString();
        } catch ( NoSuchAlgorithmException ex){
            result.put("status", 0);
            result.put("message", ex.getCause());
            return result.toJSONString();
        } catch (IOException e) {
            result.put("status", 0);
            result.put("message", e.getCause());
            return result.toJSONString();
        }
    }

    @GetMapping("/deleteFile")
    public String deleteFile(String remoteFileName) {
        JSONObject result;
        try{
            JSONObject res = fileService.deleteFile(groupName, remoteFileName);
            return res.toJSONString();
        } catch (Exception e) {
            result = new JSONObject();
            result.put("status", 0);
            result.put("message", e.getCause());
            e.printStackTrace();
            return result.toJSONString();
        }
    }

    @GetMapping("/getFileInfo")
    public String getFileInfo(String remoteFileName) {
        return fileService.getFileInfo(groupName, remoteFileName).toJSONString();
    }


    @GetMapping("/downloadFile")
    public ResponseEntity downloadFile(HttpServletResponse response,
                                                    @RequestParam("remoteFileName") String remoteFileName) throws FileNotFoundException, MalformedURLException {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", "attachment; filename=" +remoteFileName);
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");
        headers.add("Last-Modified", new Date().toString());
        headers.add("ETag", String.valueOf(System.currentTimeMillis()));
        String filePath = fileService.getFilePath(groupName, remoteFileName);
        System.out.println(filePath);
        return  ResponseEntity
                .ok()
                .headers(headers)
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(new UrlResource(filePath));
    }


}
