package org.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * UnsplittableFileEntry.
 *
 * Holds some filename information. To be used as key.
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 01 Sep 2014
 * @since 1.6.x
 */
public class UnsplittableFileEntry implements WritableComparable<UnsplittableFileEntry> {

    private String filename = null;
    
    private long line = -1L;
    
    /**
     * Default constructor required by Hadoop.
     */
    public UnsplittableFileEntry() {}
    
    public void clear() {
        filename = null;
        line = -1L;
    }

    @Override
    public int compareTo(UnsplittableFileEntry o) {
        int comp = this.filename.compareTo(o.filename);
        if (comp != 0) return comp;
        
        if (this.line < o.line) return -1;
        else if (this.line == o.line) return 0;
        else return 1;
    }
    
    public String getFilename() {
        return filename;
    }

    public long getLine() {
        return line;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.filename = in.readUTF();
        this.line = in.readLong();
    }
    
    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setLine(long line) {
        this.line = line;
    }
    
    @Override
    public String toString() {
        return filename + "/" + line;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(filename);
        out.writeLong(line);
    }
}
