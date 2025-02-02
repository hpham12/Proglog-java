package org.proglog.internal.log;

import java.io.*;
import java.math.BigInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class to represent the Store component of the log
 *
 * @author hpham
 * */
public class Store {
    private final RandomAccessFile dataFile;
    private final ReadWriteLock lock;
    private final BufferedOutputStream bufferedWriter;
    private final DataOutputStream dataLengthWriter;
    private long size;
    // number of bytes used to store the record's length
    public static int LEN_WIDTH_BYTES = 4;

    private Store(RandomAccessFile dataFile, ReadWriteLock lock, BufferedOutputStream bufferedWriter, long size) {
        this.dataFile = dataFile;
        this.lock = lock;
        this.bufferedWriter = bufferedWriter;
        this.dataLengthWriter = new DataOutputStream(bufferedWriter);
        this.size = size;
    }

    static Store newStore(RandomAccessFile dataFile) throws IOException {
        return new Store(
                dataFile,
                new ReentrantReadWriteLock(),
                new BufferedOutputStream(new FileOutputStream(dataFile.getFD())),
                dataFile.length()
        );
    }

    /**
     * Method to append data into the log
     *
     * @Param data to add
     * @Return position of the new data, -1 if the append operation failed
     * */
    public long append(byte[] data) {
        lock.writeLock().lock();
        long positionOfNewData = size;
        try {
            // write the length of the data
            dataLengthWriter.writeInt(data.length);
            size += LEN_WIDTH_BYTES;
            bufferedWriter.write(data);
            size += data.length;
            return positionOfNewData;
        } catch (IOException e) {
            // TODO: replace this with proper logging
            System.err.println(e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }

        return -1;
    }

    /**
     * Read data, given a <code>position</code>
     * */
    public byte[] read(long position) throws IOException {
        lock.readLock().lock();
        try {
            bufferedWriter.flush();
            dataFile.seek(position);
            byte[] lengthBytes = new byte[LEN_WIDTH_BYTES];
            dataFile.read(lengthBytes, 0, LEN_WIDTH_BYTES);
            int dataSize = new BigInteger(lengthBytes).intValue();
            byte[] dataBytes = new byte[dataSize];
            dataFile.seek(position + LEN_WIDTH_BYTES);
            dataFile.read(dataBytes, 0, dataSize);
            return dataBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Read N bytes starting at a specific <code>offset</code> into <code>outputBytes</code>,
     * where N is the size of <code>outputBytes</code>
     * */
    public int readAt(byte[] outputBytes, int offset) throws IOException {
        lock.readLock().lock();
        try {
            bufferedWriter.flush();
            dataFile.seek(offset);
            return dataFile.read(outputBytes, 0, outputBytes.length);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Close the resources
     * */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            bufferedWriter.flush();
            dataFile.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
