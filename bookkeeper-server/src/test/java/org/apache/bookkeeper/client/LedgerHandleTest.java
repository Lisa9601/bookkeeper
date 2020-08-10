package org.apache.bookkeeper.client;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Enumeration;

import static org.junit.Assert.*;

public class LedgerHandleTest {

    LedgerHandle ledger = null;
    long entry1, entry2, entry3;

    @Before
    public void setUp() throws Exception {

        String connectionString = "127.0.0.1:2181";
        BookKeeper bkClient = new BookKeeper(connectionString);
        byte[] password = "password".getBytes();
        this.ledger = bkClient.createLedger(BookKeeper.DigestType.MAC, password);

        entry1 = ledger.addEntry("hello".getBytes());
        entry2 = ledger.addEntry("world".getBytes());
        entry3 = ledger.addEntry("!".getBytes());

    }

    @Test
    public void readEntriesTest() {

        Enumeration<LedgerEntry> entries = null;
        LedgerEntry entry = null;

        try {
            entries = ledger.readEntries(entry1,entry3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }

        int i = 0;

        while(entries.hasMoreElements()){

            entry = entries.nextElement();
            i++;
        }

        assertEquals(3,i);

    }

}