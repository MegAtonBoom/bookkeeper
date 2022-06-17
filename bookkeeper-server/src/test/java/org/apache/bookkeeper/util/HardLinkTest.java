package org.apache.bookkeeper.util;



import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.apache.bookkeeper.client.BufferedChannelTest;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;


@RunWith(Parameterized.class)
public class HardLinkTest {

    private String tmpPath;
    private String validFileName="test.txt";
    private File validFile;

    private FileType createSource;
    private FileType createTarget;
    private FileType fileToCheck;

    private File createSourceFile;
    private File createTargetFile;
    private File fileToCheckFile;
    private int expected;


    public void initialConfig() throws IOException {
        //get path
        String currentPath = new File(".\\").getCanonicalPath();
        this.tmpPath = currentPath+"\\tmpDir";
        //create the temp directory
        new File(tmpPath).mkdirs();
        //create the temp valid file;
        //this.validFile = new File(this.currentPath+"\\"+this.validFileName);
    }

    public HardLinkTest(FileType createSource,FileType createTarget, FileType fileToCheck, int expected) throws IOException {
        initialConfig();
        configureCreateHardLinkParams(createSource, createTarget);
        configureGetLinkCountParam(fileToCheck, expected);
    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][]{
                //createHardLink            getLinkCount
                //sourceFile fileType       targetFile fileType       fileType to check     espected result
                {FileType.EXISTING,         FileType.NOT_EXISTING,    FileType.EXISTING,        1}

        });
    }

    private void configureCreateHardLinkParams(FileType createSource, FileType createTarget) throws IOException {
        this.createSourceFile = new File(this.tmpPath+"\\source.txt");
        this.createTargetFile = new File(this.tmpPath+"\\target.txt");
        System.out.println(this.tmpPath+"\\target.txt");
        switch(createSource){
            case EXISTING:
                this.createSourceFile.createNewFile();
                break;

            case NULL: this.createSourceFile = null;
                break;

            default: break;
        }

        switch(createTarget){
            case EXISTING:
                this.createTargetFile.createNewFile();
                break;

            case NULL: this.createTargetFile = null;
                break;

            default: break;
        }
    }

    private void configureGetLinkCountParam(FileType fileToCheck, int expected) throws IOException {
        this.fileToCheckFile = new File(this.tmpPath+"\\check.txt");
        this.expected=expected;
        switch(fileToCheck){
            case EXISTING:
                this.fileToCheckFile.createNewFile();
                break;

            case NULL: this.createTargetFile = null;
                break;

            default: break;
        }

    }

    @Test
    public void testCreateHardLink(){
        try {
            HardLink.createHardLink(this.createSourceFile, this.createTargetFile);
        }
        catch(IOException ioe){
            System.out.print("test");
        }
    }

    @Test
    public void testGetLinkCount(){
        int count;
        this.fileToCheckFile.setReadable(true,true);
        this.fileToCheckFile.setExecutable(true,true);
        HardLink hl = new HardLink();
        System.out.println(this.fileToCheckFile.exists());
        try{
            count = hl.getLinkCount(this.fileToCheckFile);
            System.out.println("got "+count+" but you told me "+this.expected);
            Assert.assertEquals(count, this.expected);
        }
        catch(IOException ioe){
            ioe.printStackTrace();
            Assert.assertTrue(this.fileToCheck==FileType.NULL || this.fileToCheck==FileType.NOT_EXISTING);
        }
        catch(NullPointerException npe){
            Assert.assertTrue(this.fileToCheck == FileType.NULL);
        }
    }


    private enum FileType{
        NULL,
        EXISTING,
        NOT_EXISTING;
    }

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(this.tmpPath+"\\source.txt"));
        Files.deleteIfExists(Paths.get(this.tmpPath+"\\target.txt"));
        Files.deleteIfExists(Paths.get(this.tmpPath+"\\check.txt"));
        Files.deleteIfExists(Paths.get(this.tmpPath));
    }


}
