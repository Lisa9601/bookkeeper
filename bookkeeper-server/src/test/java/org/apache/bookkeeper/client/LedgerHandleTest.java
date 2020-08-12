package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Enumeration;

import static org.junit.Assert.*;

public class LedgerHandleTest extends BookKeeperClusterTestCase {

    LedgerHandle ledger = null;
    long entry1, entry2, entry3;

    public LedgerHandleTest() {
        super(3);
    }

    @Override
    @Before
    public void setUp() throws Exception {

        super.setUp();

        byte[] password = "password".getBytes();
        this.ledger = bkc.createLedger(1,1,BookKeeper.DigestType.MAC, password);

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