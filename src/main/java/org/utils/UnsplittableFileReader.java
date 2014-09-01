package org.utils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * UnsplittableFileReader.
 *
 * Outputs for a file a key/value pair where the key is the file name and line number,
 * and the value is the content of the current line.
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 01 Sep 2014
 * @since 1.6.x
 */
public class UnsplittableFileReader extends RecordReader<UnsplittableFileEntry, Text> {
    
    private static final Log LOG = LogFactory.getLog(UnsplittableFileReader.class);
        
    private String filename = null;
    
    private LineRecordReader reader = null;
    
    private UnsplittableFileEntry key = null;

    private Text value = null;

    public UnsplittableFileReader() {}

    @Override
    public synchronized void close() throws IOException {
        if (reader != null) reader.close();
    }

    @Override
    public synchronized boolean nextKeyValue() throws IOException {
        boolean res = reader.nextKeyValue();
        if (res) {
            LongWritable lineNumber = reader.getCurrentKey();
            Text lineString = reader.getCurrentValue();
            
            key.clear();
            key.setFilename(filename);
            key.setLine(lineNumber.get());
            
            value.clear();
            value.set(lineString.copyBytes());
            
            LOG.debug("read " + key);
        }
        
        return res;
    }

    @Override
    public synchronized UnsplittableFileEntry getCurrentKey() {
        return key;
    }

    @Override
    public synchronized Text getCurrentValue() {
        return value;
    }

    @Override
    public synchronized float getProgress() throws IOException {
        return reader.getProgress();
    }

    
    @Override
    public void initialize(InputSplit isplit, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            key = new UnsplittableFileEntry();
            value = new Text();

            FileSplit split = (FileSplit) isplit;
            Path file = split.getPath();
            filename = file.getName();
            
            reader = null;
            reader = new LineRecordReader();
            reader.initialize(split, context);
        }
        catch (IOException ex) {
            Logger.getLogger(UnsplittableFileReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
