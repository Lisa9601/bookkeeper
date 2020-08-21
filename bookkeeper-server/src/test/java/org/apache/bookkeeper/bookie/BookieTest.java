package org.apache.bookkeeper.bookie;

import com.google.common.base.Charsets;
import io.netty.buffer.*;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class BookieTest extends BookKeeperClusterTestCase {

    LedgerHandle ledger = null;
    long entryId = 0;

    public BookieTest() {
        super(3);
    }


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        ledger = bkc.createLedger(BookKeeper.DigestType.CRC32,"password".getBytes());

    }

    @Test
    public void addEntryTest1(){

        Bookie bookie = bs.get(0).getBookie();
        ByteBuf buf = Unpooled.buffer();
        buf.writeLong(ledger.getId());
        buf.writeBytes("entry".getBytes());

        BookkeeperInternalCallbacks.WriteCallback wc = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {

            }
        };

        try {
            bookie.addEntry(buf,false,wc,null,ledger.getLedgerKey());
        } catch (IOException | BookieException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    /*
    @Test
    public void readLastAddConfirmedTest() throws BKException, InterruptedException {

        Bookie bookie = bs.get(0).getBookie();
        long id = 0;

        entryId = ledger.addEntry("hello world!".getBytes());

        try {
            bookie.waitForLastAddConfirmedUpdate(ledger.getId(), -1, new Watcher<LastAddConfirmedUpdateNotification>() {
                @Override
                public void update(LastAddConfirmedUpdateNotification lastAddConfirmedUpdateNotification) {

                }
            });

            id = bookie.readLastAddConfirmed(ledger.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }


        assertEquals(entryId,id);
    }

    @Test
    public void readEntryTest() throws BKException, InterruptedException {

        Bookie bookie = bs.get(0).getBookie();
        ByteBuf byteBuf = null;

        entryId = ledger.addEntry("hello world!".getBytes());


        try {
            byteBuf = bookie.readEntry(ledger.getId(),entryId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readableBytes()-12,bytes);
        assertEquals("hello world!", new String(bytes));

    }
    */


}
