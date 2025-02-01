package org.proglog.internal.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StoreTest {
    private RandomAccessFile raf;
    private File file;
    private Store store;

    @BeforeEach
    void setup() throws IOException {
        file = File.createTempFile("test", ".store");
        raf = new RandomAccessFile(file, "rw");
        store = Store.newStore(raf);
    }

    @AfterEach
    void cleanup() throws IOException{
        raf.close();
        boolean deletedSuccessfully = file.delete();
        if (!deletedSuccessfully) {
            throw new RuntimeException();
        }
    }

    @Test
    void testAppendAndRead() throws IOException {
        String testString1 = "Hello World 1";
        String testString2 = "Hello World 2";
        long position1 = store.append(testString1.getBytes());
        long position2 = store.append(testString2.getBytes());
        assertEquals(testString1, new String(store.read(position1)));
        assertEquals(testString2, new String(store.read(position2)));
    }

    @Test
    void testReadAt() throws IOException {
        String testString = "Hello World";
        store.append(testString.getBytes());
        byte[] outputBytes = new byte[testString.length()];
        int readBytes = store.readAt(outputBytes, Store.LEN_WIDTH_BYTES);
        assertEquals(outputBytes.length, readBytes);
        assertEquals(testString, new String(outputBytes));
    }

    @Test
    void testCloseAndReopen() throws IOException {
        store.append("Hello World 1".getBytes());
        assertDoesNotThrow(() -> store.close());

        raf = new RandomAccessFile(file, "rw");
        store = Store.newStore(raf);
        assertEquals("Hello World 1", new String(store.read(0)));
    }
}
