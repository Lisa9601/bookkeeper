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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class AddEntryTest extends BookKeeperClusterTestCase {

    private long ledgerId;
    private final long entryId = 20;
    private static final String string = "text for tests";

    private static final byte[] masterKey = "password".getBytes();
    private static final BookkeeperInternalCallbacks.WriteCallback wc = (rc, ledgerId, entryId, addr, ctx) -> { };

    private ByteBuf entry;
    private boolean ack;
    private BookkeeperInternalCallbacks.WriteCallback writeCallback;
    private Object ctx;
    private byte[] key;
    private boolean valid;
    private boolean expected;

    @Parameterized.Parameters
    public static Collection params() {

        return Arrays.asList(new Object[][] {
                { Unpooled.buffer(), false, wc, null, masterKey, true, true },
                { Unpooled.buffer(), true, wc, null, null, false, false },
                { null, false, null, null, masterKey, false, false }
        });
    }


    public AddEntryTest(ByteBuf entry, boolean ack, BookkeeperInternalCallbacks.WriteCallback writeCallback,
                        Object ctx, byte[] key, boolean valid, boolean expected) {
        super(3);
        this.entry = entry;
        this.ack = ack;
        this.writeCallback = writeCallback;
        this.ctx = ctx;
        this.key = key;
        this.valid = valid;
        this.expected = expected;
    }


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        LedgerHandle ledger = bkc.createLedger(BookKeeper.DigestType.CRC32, masterKey);
        ledgerId = ledger.getId();

        if(valid){
            entry.writeLong(ledgerId);
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

        if(valid){

            try {
                ByteBuf entryRead = bookie.readEntry(ledgerId,entryId);

                byte[] destination = new byte[entryRead.readableBytes()];
                entryRead.getBytes(0,destination);
                String content = new String(destination);
                content = content.substring(content.length() - string.length());

                Assert.assertEquals(string,content);

            } catch (IOException e) {
                e.printStackTrace();
                result = false;
            }

        }

        Assert.assertEquals(expected,result);

    }


}
