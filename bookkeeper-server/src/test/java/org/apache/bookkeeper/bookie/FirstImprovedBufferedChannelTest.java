package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


@RunWith(Parameterized.class)
public class FirstImprovedBufferedChannelTest{

    private ByteBuf writeBuf;
    private int buffSize=256;
    private File tmpFile;
    private FileChannel fc;

    //tested instance
    private BufferedChannel bc;

    //method params
    private ByteBuf destBuf;
    private int pos;
    private int length;

    //expected result
    private int exp;

    private boolean expectedIllegalArgument;
    private boolean expectedNullPointer;
    private boolean expectedIO;


    public FirstImprovedBufferedChannelTest(Position writeBufPos, FileType file, BytebufType bbt, int pos, int length, int exp) throws IOException {

        configure(writeBufPos, file , bbt, pos, length, exp);

    }

    private void configure(Position writeBufPos, FileType file, BytebufType bbt, int pos, int length, int exp) throws IOException {

        File currentDir = new File(".\\");
        this.tmpFile = new File(currentDir+"\\"+"test.txt");
        if(bbt==BytebufType.NULL) this.expectedNullPointer = true;
        if(writeBufPos==Position.SUP && file==FileType.EMPTY) this.expectedIO=true;
        if(pos<0) this.expectedIllegalArgument = true;
        if( length>this.buffSize || length> (this.buffSize-pos)) this.expectedIO = true;


        this.writeBuf = Unpooled.buffer(0, this.buffSize);
        byte [] data = new byte[this.buffSize];
        Random random = new Random();
        random.nextBytes(data);
        this.writeBuf.writeBytes(data);



        this.exp=exp;
        this.pos=pos;
        this.length=length;
        switch(bbt){
            case EMPTY: this.destBuf = Unpooled.buffer(0); break;
            case VALID: this.destBuf = Unpooled.buffer( this.buffSize); break;
            default: this.destBuf = null; break;
        }


        FileWriter Writer = new FileWriter(this.tmpFile);
        switch(file){
            case FILLED:
             Writer.write("25 test pourpouse bytesss");
             break;
            default :  Writer.write(""); break;

        }
        Writer.close();



        this.fc = new RandomAccessFile(tmpFile, "rw").getChannel();


        this.bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fc, this.buffSize*2, 0);

        this.bc.write(writeBuf);

        if(writeBufPos == Position.SUP){
            this.bc.writeBufferStartPosition = new AtomicLong(pos+1);
        }

    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][]{
                //ByteBuf type          pos         length     expected return
                {Position.INF, FileType.EMPTY,      BytebufType.NULL,     -1,          5,         0},
                {Position.INF, FileType.EMPTY,      BytebufType.VALID,     257,        -10,       0},
                {Position.INF, FileType.EMPTY,      BytebufType.EMPTY,     0,          257,       0},

                //added to improve jacoco coverage
                {Position.INF, FileType.EMPTY,      BytebufType.VALID,     231,          25,      25},
                {Position.INF, FileType.EMPTY,      BytebufType.NULL,      0,          25,        0},

                //added to improve ba-dua coverage and cover new test cases

                {Position.SUP, FileType.FILLED,      BytebufType.VALID,     0,          25,        25},
                {Position.SUP, FileType.EMPTY,      BytebufType.VALID,     231,          25,        0},


        });
    }


    @Test
    public void testRead(){
        try{

            int copied=this.bc.read(this.destBuf, this.pos, this.length);
            Assert.assertEquals(copied, this.exp);
        }
        catch(NullPointerException ne){
            Assert.assertTrue(this.expectedNullPointer);
        }
        catch(IOException ioe){
            Assert.assertTrue(this.expectedIO);
        }
        catch(IllegalArgumentException ie){
            Assert.assertTrue(this.expectedIllegalArgument);
        }


    }

    @After
    public void tearDown(){
        this.tmpFile.deleteOnExit();
    }


    private enum BytebufType{
        VALID,
        NULL,
        EMPTY;
    }

    private enum FileType{
        EMPTY,
        FILLED
    }

    private enum Position{
        INF,
        SUP
    }



}
