package org.apache.bookkeeper.proto;


import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

public class BookieServerTest extends BookKeeperClusterTestCase {

    public BookieServerTest() {
        super(3);
    }

    @Test
    public void shutdownTest(){

        BookieServer server = bs.get(0);

        server.shutdown();

        Assert.assertFalse(server.isRunning());

    }


    @Test
    public void startTest1(){

        ServerConfiguration conf = new ServerConfiguration();
        conf.setAdvertisedAddress("127.0.0.1");
        conf.setBookiePort(2181);
        BookieServer server = null;

        try {
            server = new BookieServer(conf);
            server.start();
        } catch (IOException | KeeperException | InterruptedException | BookieException | ReplicationException.UnavailableException | ReplicationException.CompatibilityException | SecurityException e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertTrue(server.isRunning());

    }


    @Test
    public void startTest2(){

        try {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setAdvertisedAddress("127.0.0.1");
            conf.setBookiePort(2182);

            BookieServer server = new BookieServer(conf);
            Bookie spyBookie = Mockito.spy(server.getBookie());

            Mockito.doReturn(false).when(spyBookie).isRunning();

            Whitebox.setInternalState(server,"bookie",spyBookie);

            server.start();

        } catch (IOException | KeeperException | InterruptedException | BookieException | ReplicationException.UnavailableException | ReplicationException.CompatibilityException | SecurityException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void startTest3(){

        try {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setAdvertisedAddress("127.0.0.1");
            conf.setBookiePort(2183);

            BookieServer server = new BookieServer(conf);
            server.setExceptionHandler((thread, throwable) -> { });

            server.start();

        } catch (IOException | KeeperException | InterruptedException | BookieException | ReplicationException.UnavailableException | ReplicationException.CompatibilityException | SecurityException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void testSuspendResume(){

        try {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setAdvertisedAddress("127.0.0.1");
            conf.setBookiePort(2183);

            BookieServer server = new BookieServer(conf);
            server.suspendProcessing();

            Assert.assertFalse(server.isRunning());

            server.resumeProcessing();

            Assert.assertFalse(server.isRunning());

        } catch (IOException | KeeperException | InterruptedException | BookieException | ReplicationException.UnavailableException | ReplicationException.CompatibilityException | SecurityException e) {
            e.printStackTrace();
        }

    }


}
