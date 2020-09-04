package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
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

    private static final byte[] masterKey = "password".getBytes();

    private long ledgerId;
    private byte[] ledgerKey;
    private boolean expected;

    public FenceLedgerTest(long ledgerId, byte[] ledgerKey, boolean expected) {
        super(3);
        this.ledgerId = ledgerId;
        this.ledgerKey = ledgerKey;
        this.expected = expected;
    }


    @Parameterized.Parameters
    public static Collection params() {

        return Arrays.asList(new Object[][] {
                { -1, null, false },
                { 0, masterKey, true },
                { 0, null, false }
        });
    }


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        LedgerHandle ledger = bkc.createLedger(BookKeeper.DigestType.CRC32, masterKey);

        if(ledgerId == 0) {
            ledgerId = ledger.getId();
        }

    }


    @Test
    public void test(){

        Bookie bk = bs.get(0).getBookie();
        boolean result = true;

        try {
            bk.fenceLedger(ledgerId, ledgerKey);
        } catch (IOException | BookieException e) {
            e.printStackTrace();
            result = false;

        }

        if(result){

            result = false;

            try {

                ByteBuf buf = Unpooled.buffer();
                buf.writeLong(ledgerId);
                buf.writeBytes("bla bla bla".getBytes());

                BookkeeperInternalCallbacks.WriteCallback wc = new BookkeeperInternalCallbacks.WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {

                    }
                };

                bk.addEntry(buf,false,wc,null,ledgerKey);

            } catch (InterruptedException | BookieException | IOException e) {
                e.printStackTrace();
                result = true;
            }


        }

        Assert.assertEquals(expected,result);

    }

}
