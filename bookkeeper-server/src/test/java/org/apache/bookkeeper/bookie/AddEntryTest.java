package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.matchers.Null;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class AddEntryTest extends BookKeeperClusterTestCase {

    private LedgerHandle ledger = null;
    private long entryId = 20;
    private String string = "text for tests";

    private static final byte[] masterKey = "password".getBytes();
    private static final BookkeeperInternalCallbacks.WriteCallback wc = (rc, ledgerId, entryId, addr, ctx) -> { };

    private ByteBuf entry;
    private boolean ack;
    private BookkeeperInternalCallbacks.WriteCallback writeCallback;
    private Object ctx;
    private byte[] key;
    private boolean expected;

    @Parameterized.Parameters
    public static Collection params() {

        return Arrays.asList(new Object[][] {
                { Unpooled.buffer(), false, wc, null, masterKey, true },
                { null, false, wc, null, masterKey, false }
        });
    }


    public AddEntryTest(ByteBuf entry, boolean ack, BookkeeperInternalCallbacks.WriteCallback writeCallback,
                        Object ctx, byte[] key, boolean expected) {
        super(3);
        this.entry = entry;
        this.ack = ack;
        this.writeCallback = writeCallback;
        this.ctx = ctx;
        this.key = key;
        this.expected = expected;
    }


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ledger = bkc.createLedger(BookKeeper.DigestType.CRC32,masterKey);

        if(entry != null){
            entry.writeLong(ledger.getId());
            entry.writeLong(entryId);
            entry.writeBytes(string.getBytes());

        }

    }


    @Test
    public void test(){

        boolean result = true;

        Bookie bookie = bs.get(0).getBookie();

        try {
            bookie.addEntry(entry,ack,writeCallback,ctx,key);
        } catch (IOException | BookieException | InterruptedException | NullPointerException e) {
            e.printStackTrace();
            result = false;
        }

        try {
            ByteBuf entryRead = bookie.readEntry(ledger.getId(),entryId);

            byte[] destination = new byte[entryRead.readableBytes()];
            entryRead.getBytes(0,destination);
            String content = new String(destination);
            content = content.substring(content.length() - string.length());

            Assert.assertEquals(string,content);

        } catch (IOException e) {
            e.printStackTrace();
            result = false;
        }




        Assert.assertEquals(expected,result);

    }


}
