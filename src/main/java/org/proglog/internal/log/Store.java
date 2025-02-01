package org.proglog.internal.log;

import java.io.*;
import java.math.BigInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Store {
    private final RandomAccessFile dataFile;
    private final ReadWriteLock lock;
    private final BufferedOutputStream bufferedWriter;
    private final DataOutputStream dataLengthWriter;
    private long size;

    // number of bytes used to store the record's length
    public static int LEN_WIDTH_BYTES = 8;

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
        long positionOfNewData = size;
        try {
            lock.writeLock().lock();

            // write the length of the data
            dataLengthWriter.writeLong(data.length);
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
     * Read data, given a position
     *
     * @Return data at the position
     * */
    public byte[] read(long position) throws IOException {
        try {
            lock.readLock().lock();
            bufferedWriter.flush();
            dataFile.seek(position);
            byte[] lengthBytes = new byte[LEN_WIDTH_BYTES];
            dataFile.read(lengthBytes, 0, LEN_WIDTH_BYTES);
            long dataSize = new BigInteger(lengthBytes).longValue();
            byte[] dataBytes = new byte[(int) dataSize];
            dataFile.seek(position + LEN_WIDTH_BYTES);
            dataFile.read(dataBytes, 0, (int) dataSize);
            return dataBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int readAt(byte[] outputBytes, long offset) throws IOException {
        try {
            lock.readLock().lock();
            bufferedWriter.flush();
            dataFile.seek(offset);
            return dataFile.read(outputBytes, 0, outputBytes.length);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() throws IOException {
        try {
            lock.writeLock().lock();
            bufferedWriter.flush();
            dataFile.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
