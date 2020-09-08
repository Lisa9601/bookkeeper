package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.zookeeper.KeeperException;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class StartTest {

    private BookieServer server;
    private boolean expected;

    private static BookieServer server1, server2, server3;

    public StartTest(BookieServer server, boolean expected) {
        this.server = server;
        this.expected = expected;
    }


    @AfterClass
    public static void tearDown() throws Exception {

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();

    }


    @Parameterized.Parameters
    public static Collection params() {

        ServerConfiguration conf1 = new ServerConfiguration();
        conf1.setAdvertisedAddress("127.0.0.1");
        conf1.setBookiePort(5000);

        ServerConfiguration conf2 = new ServerConfiguration();
        conf2.setAdvertisedAddress("127.0.0.1");
        conf2.setBookiePort(5001);

        ServerConfiguration conf3 = new ServerConfiguration();
        conf3.setAdvertisedAddress("127.0.0.1");
        conf3.setBookiePort(5002);

        try{
            server1 = new BookieServer(conf1);

            server2 = new BookieServer(conf2);
            Bookie spyBookie = Mockito.spy(server2.getBookie());
            Mockito.doReturn(false).when(spyBookie).isRunning();
            Whitebox.setInternalState(server2,"bookie",spyBookie);

            server3 = new BookieServer(conf3);
            server3.setExceptionHandler((thread, throwable) -> { });

        } catch (ReplicationException.CompatibilityException | ReplicationException.UnavailableException | KeeperException | BookieException | IOException | SecurityException | InterruptedException e) {
            e.printStackTrace();
        }

        return Arrays.asList(new Object[][] {
                { server1, true },
                { server2, false },
                { server3, true }
        });
    }


    @Test
    public void test(){

        try {
            server.start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(expected,server.isRunning());

    }


}
