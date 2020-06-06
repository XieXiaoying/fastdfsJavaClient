package com.fastdfs.filetools.entity;

public class FastDFSFile {
    private String name;

    private byte[] content;

    private String ext;

    private String md5;

    public FastDFSFile(String name, byte[] content, String ext, String md5) {
        super();
        this.name = name;
        this.content = content;
        this.ext = ext;
        this.md5 = md5;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public String getExt() {
        return ext;
    }

    public void setExt(String ext) {
        this.ext = ext;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }
}
