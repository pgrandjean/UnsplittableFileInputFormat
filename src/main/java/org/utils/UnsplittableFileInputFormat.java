package org.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * UnsplittableFileInputFormat.
 *
 * The input file is set as non-splittable.
 * The key/value generation delegated to {@link UnsplittableFileReader}
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 01 Sep 2014
 * @since 1.6.x
 */
public class UnsplittableFileInputFormat extends FileInputFormat<UnsplittableFileEntry, Text> {

    public UnsplittableFileInputFormat() {
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
    
    @Override
    public RecordReader<UnsplittableFileEntry, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new UnsplittableFileReader();
    }
}
