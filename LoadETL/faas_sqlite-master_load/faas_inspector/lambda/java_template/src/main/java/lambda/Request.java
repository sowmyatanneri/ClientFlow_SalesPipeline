/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

/**
 *
 * @author wlloyd
 */
public class Request {
    String bucketname;
    String filename;
    int seqNum;
    int threadId;
    public String getBucketname()
    {
        return bucketname;
    }
    public void setBucketname(String bucketname)
    {
        this.bucketname = bucketname;
    }
    public String getFilename()
    {
        return filename;
    }
    public void setFilename(String filename)
    {
        this.filename = filename;
    }
    public int getseqNum()
    {
        return seqNum;
    }
    public void setseqNum(int seqNum)
    {
        this.seqNum = seqNum;
    }
    public int getthreadId()
    {
        return threadId;
    }
    public void setthreadId(int threadId)
    {
        this.threadId = threadId;
    }
    
    public Request(String filename,String bucketname)
    {
        this.filename = filename;
        this.bucketname = bucketname;
    }
    public Request()
    {
        
    }
}
