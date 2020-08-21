package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class FenceLedgerTest extends BookKeeperClusterTestCase {

    private LedgerHandle ledger = null;
    private long entryId = 0;
    private long ledgerId;
    private byte[] ledgerKey;
    private int expected;
    private static final byte[] masterKey = "password".getBytes();

    public FenceLedgerTest(long ledgerId, byte[] ledgerKey, int expected) {
        super(3);
        this.ledgerId = ledgerId;
        this.ledgerKey = ledgerKey;
        this.expected = expected;
    }


    @Parameterized.Parameters
    public static Collection params() {

        return Arrays.asList(new Object[][] {
                { -1, null, 1 },
                { 0, masterKey, 2 },
        });
    }


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        ledger = bkc.createLedger(BookKeeper.DigestType.CRC32, masterKey);

        if(ledgerId == 0) {
            ledgerId = ledger.getId();
        }

    }


    @Test
    public void fenceLedgerTest(){

        Bookie bk = bs.get(0).getBookie();
        int result = 0;

        try {
            bk.fenceLedger(ledgerId, ledgerKey);
        } catch (IOException | BookieException e) {
            e.printStackTrace();
            result = 1;
        }

        try {
            ledger.addEntry("bla bla bla".getBytes());
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
            result = 2;
        }

        Assert.assertEquals(expected,result);

    }

}
