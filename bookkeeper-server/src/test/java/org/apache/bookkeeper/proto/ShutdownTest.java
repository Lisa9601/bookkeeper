package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class ShutdownTest extends BookKeeperClusterTestCase {

    public ShutdownTest() {
        super(3);
    }

    @Test
    public void shutdownTest1(){

        BookieServer server = bs.get(0);

        Whitebox.setInternalState(server,"running",false);

        server.shutdown();

        if(!server.isRunning()){
            Assert.assertEquals(true,server.isBookieRunning());
        }
        else{
            Assert.fail();
        }

    }


    @Test
    public void shutdownTest2(){

        BookieServer server = bs.get(0);

        server.shutdown();

        if(!server.isRunning()){
            Assert.assertEquals(false, server.isBookieRunning());
        }
        else {
            Assert.fail();
        }
    }




}
