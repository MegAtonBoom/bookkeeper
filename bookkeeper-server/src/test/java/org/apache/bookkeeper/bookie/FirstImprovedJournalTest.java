package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class FirstImprovedJournalTest {

    private String juPath;
    private String lgPath;
    private final String ju="ju";
    private final String lg="lg";
    private File juDir;
    private File lgDir;
    private BookieImpl bookie;
    private ArrayList<Long> journalIds;
    private boolean expectedNullPointer;
    private boolean expectedIO;


    //tested instance
    private Journal journal;

    //method params
    long id;
    long pos;
    Journal.JournalScanner js;


    private void initialConfig() throws Exception {

        String currentPath = new File(".\\").getCanonicalPath();
        this.juPath = currentPath+"\\"+this.ju;
        this.lgPath = currentPath+"\\"+this.lg;

        this.juDir = new File(this.juPath);
        new File(this.juPath).mkdirs();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(this.juDir));

        this.lgDir = new File(this.lgPath);
        new File(this.lgPath).mkdirs();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(this.lgDir));

        JournalUtil.writeV4Journal(BookieImpl.getCurrentDirectory(this.juDir), 10, "abcde".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        //care
        conf.setJournalDirName(this.juPath)
                .setLedgerDirNames(new String[] { this.lgPath })
                .setMetadataServiceUri(null);

        this.bookie = new TestBookieImpl(conf);
        this.journal=this.bookie.journals.get(0);

    }



    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //id parameter type      pos parameter type         scanner parameter type   expected IOE   expected NPE
                { ParamType.VALID,       ParamType.NOT_VALID,       ParamType.NULL,          true,          true },
                { ParamType.NOT_VALID,   ParamType.VALID,           ParamType.VALID,         false,         true },
                { ParamType.NOT_VALID,   ParamType.NOT_VALID,       ParamType.NOT_VALID,     false,         true },

                //added to improve jacoco coverage
                { ParamType.VALID,       ParamType.VALID,           ParamType.VALID,         false,         false },
                { ParamType.NOT_VALID,   ParamType.NOT_VALID,       ParamType.VALID,     false,         true },

                //added to improve badua coverage
                { ParamType.VALID,       ParamType.VALID0,           ParamType.VALID,         false,         false },
                { ParamType.NOT_VALID,   ParamType.NOT_VALID,       ParamType.VALID,     false,         true },
        });
    }

    public FirstImprovedJournalTest(ParamType id, ParamType pos, ParamType scanner, boolean expectedIO, boolean expectedNullPointer) throws Exception {
        configure(id, pos, scanner, expectedIO, expectedNullPointer);
    }

    public void configure(ParamType id, ParamType pos, ParamType scanner, boolean expectedIO, boolean expectedNullPointer ) throws Exception {


        initialConfig();
        this.expectedIO = expectedIO;
        this.expectedNullPointer = expectedNullPointer;
        this.journalIds = (ArrayList<Long>)Journal.listJournalIds(this.journal.getJournalDirectory(), null);
        switch(id){
            case VALID: this.id = this.journalIds.get(0); break;
            case NOT_VALID: setNotValidID(); break;
            default: this.id = 0;
        }
        switch(pos){
            case VALID: this.pos = 1; break;
            case VALID0: this.pos = -1; break;
            default: this.pos = this.journal.maxJournalSize+1;
        }
        this.js=getScanner(scanner);
    }

    private Journal.JournalScanner getScanner(ParamType scanner) throws IOException {
        Journal.JournalScanner js;
        switch(scanner){
            case VALID:
                js =  mock(Journal.JournalScanner.class);
                doNothing().when(js).process(anyInt(), anyLong(),  isA(ByteBuffer.class));
                return js;

            case NOT_VALID:
                js =  mock(Journal.JournalScanner.class);
                doThrow(RuntimeException.class).when(js).process(anyInt(), anyLong(),  isA(ByteBuffer.class));
                return js;
            default: return null;
        }

    }

    private void setNotValidID(){
        boolean valid=false;
        for(long i=0; valid==false; i++){
            if(!this.journalIds.contains(i))
            {
                valid=true;
                this.id=i;
            }
        }

    }

    @Test
    public void testScanJournal(){
        try{
            long read;
            read=this.journal.scanJournal(this.id, this.pos, this.js);
            Assert.assertTrue(read>=0);
        }
        catch(IOException ioe){
            Assert.assertTrue(this.expectedIO);
        }
        catch(NullPointerException npe){
            Assert.assertTrue(this.expectedNullPointer);
        }
    }

    @After
    public void tearDown() throws IOException {
        this.bookie.shutdown();
        deleteDir(this.juDir);
        deleteDir(this.lgDir);

    }

    private void deleteDir(File dir) {

        if(dir.isDirectory()) {
            File[] files = dir.listFiles();

            if(files != null) {
                for(File file : files) {
                    deleteDir(file);
                }
            }
        }
        dir.delete();
    }



    private enum ParamType{
        VALID,
        VALID0,
        NOT_VALID,
        NULL,
    }


}