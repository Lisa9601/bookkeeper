package org.apache.bookkeeper.bookie;

import org.junit.*;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CheckDirectoryStructureTest {

    public static File dir1, dir2, dir3;


    @BeforeClass
    public static void setUp() throws Exception {

        //Creazione delle directory
        dir1 = new File("src/test/prova");
        dir2 = new File("src/test/prova2");
        dir3 = new File("src/test/prova3");

        if(!dir1.mkdir() || !dir2.mkdir() || !dir3.mkdir()){
            throw new IOException("Failed to create directory");
        }

    }


    @AfterClass
    public static void tearDown() throws Exception {

        //Cancellazione delle directory
        if(!dir1.delete() || !dir2.delete() || !dir3.delete()){
            throw new IOException("Failed to delete directory");
        }
    }


    //La directory esiste
    @Test
    public void test1() {

        boolean result = true;

        File dir = new File("src/test/java/");
        try {
            Bookie.checkDirectoryStructure(dir);
        } catch (IOException e) {
            e.printStackTrace();
            result = false;
        }

        Assert.assertTrue(result);

    }


    //La directory non esiste
    @Test
    public void test2() throws IOException {

        boolean result = false;

        File verFile = new File("src/test/prova/VERSION");
        verFile.createNewFile();

        File dir = new File("src/test/prova/prova");
        dir.delete();

        try {
            Bookie.checkDirectoryStructure(dir);
        } catch (IOException e) {
            e.printStackTrace();
            result = true;
        }

        Assert.assertTrue(result);

        verFile.delete();
    }

    //Esistono vecchi dati nella directory
    @Test
    public void test3() throws IOException {

        boolean result = false;
        List<File> files = new ArrayList<>();

        File file = new File("src/test/prova2/prova.txn");
        file.createNewFile();
        files.add(file);
        file = new File("src/test/prova2/prova.idx");
        file.createNewFile();
        files.add(file);
        file = new File("src/test/prova2/prova.log");
        file.createNewFile();
        files.add(file);

        File dir = new File("src/test/prova2/prova");
        dir.delete();

        for (File i : files) {
            try {
                Bookie.checkDirectoryStructure(dir);
            } catch (IOException e) {
                e.printStackTrace();
                result = true;
            }

            Assert.assertTrue(result);

            result = false;
            i.delete();
        }

    }


    //Non riesce a creare le directory
    @Test
    public void test4() {

        boolean result = false;

        File dir = new File("src/test/prova3/prova");

        File dirMock = Mockito.spy(dir);

        dir.delete();

        Mockito.doReturn(false).when(dirMock).mkdirs();

        try {
            Bookie.checkDirectoryStructure(dirMock);
        } catch (IOException e) {
            e.printStackTrace();
            result = true;
        }catch (NullPointerException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(result);

    }

}
