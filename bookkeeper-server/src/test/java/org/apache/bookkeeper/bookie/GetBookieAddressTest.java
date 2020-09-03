package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class GetBookieAddressTest {

    private ServerConfiguration conf;
    private boolean expected;


    @Parameterized.Parameters
    public static Collection params() {

        ServerConfiguration advertisedConf = new ServerConfiguration();
        advertisedConf.setAdvertisedAddress("127.0.0.1");

        ServerConfiguration validConf = new ServerConfiguration();
        validConf.setListeningInterface("lo");
        validConf.setBookiePort(2181);
        validConf.setAllowLoopback(true);

        try {
            BookieServer server = new BookieServer(validConf);
            server.start();
        } catch (IOException | KeeperException | InterruptedException | BookieException | ReplicationException.UnavailableException | ReplicationException.CompatibilityException | SecurityException e) {
            e.printStackTrace();
        }

        return Arrays.asList(new Object[][] {
                { advertisedConf, true },
                { validConf, true },
                { new ServerConfiguration(), false },
                { null, false }
        });
    }

    public GetBookieAddressTest(ServerConfiguration conf, boolean expected) {
        this.conf = conf;
        this.expected = expected;
    }


    @Test
    public void getBookieAddressTest(){

        boolean result = true;

        try {
            Bookie.getBookieAddress(conf);
        } catch (UnknownHostException | NullPointerException e) {
            e.printStackTrace();
            result = false;
        }

        Assert.assertEquals(expected,result);

    }

}
