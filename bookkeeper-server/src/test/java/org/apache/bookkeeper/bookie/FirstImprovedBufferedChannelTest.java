package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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

    private boolean expectedArgumentExc;
    private boolean expectedNullPointer;
    private boolean expectedIO;


    public FirstImprovedBufferedChannelTest(Position writeBufPos, SourceType source, BytebufType bbt, int pos, int length, int exp) throws IOException {

        configure(writeBufPos, source , bbt, pos, length, exp);

    }

    private void configure(Position writeBufPos, SourceType source, BytebufType bbt, int pos, int length, int exp) throws IOException {

        File currentDir = new File(".\\");
        this.tmpFile = new File(currentDir+"\\"+"test.txt");
        if(bbt==BytebufType.NULL) this.expectedNullPointer = true;
        if(writeBufPos==Position.SUP && source== SourceType.EMPTY) this.expectedIO=true;
        if(pos<0 || pos>this.buffSize) this.expectedArgumentExc = true;
        if( length>this.buffSize || length> (this.buffSize-pos)) this.expectedIO = true;
        if(writeBufPos == Position.SUP && source == SourceType.EMPTY) this.expectedIO = true;

        byte [] data = new byte[this.buffSize];
        Random random = new Random();
        random.nextBytes(data);


        this.writeBuf = Unpooled.buffer(0, this.buffSize);
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
        switch(source){
            case FILLED:
                Writer.write( new String(data, StandardCharsets.UTF_8));
                this.fc = new RandomAccessFile(tmpFile, "rw").getChannel();


                break;
            case EMPTY :  Writer.write("");
                this.fc = new RandomAccessFile(tmpFile, "rw").getChannel();

                break;

            case NOT_VALID: this.fc = getMockedFileChannel();
                this.expectedArgumentExc = true;
                break;

            default:
                this.fc = null;
                this.expectedNullPointer=true;
                break;

        }
        Writer.close();

        try {
            this.bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fc, this.buffSize * 2, 0);
            this.bc.write(writeBuf);

            if(writeBufPos == Position.SUP){
                this.bc.writeBufferStartPosition = new AtomicLong(pos+1);
            }

        }
        catch(NullPointerException npe){
            Assert.assertTrue(this.expectedNullPointer);
        }



    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][]{
                //File or writeBuff source type             ByteBuf type          pos         length     expected return
                {Position.INF,      SourceType.FILLED,      BytebufType.NULL,     -1,          1,         0},
                {Position.INF,      SourceType.FILLED,      BytebufType.VALID,     -1,         -1,       0},
                {Position.INF,      SourceType.FILLED,      BytebufType.EMPTY,     -1,          257,       0},

                {Position.INF,      SourceType.FILLED,      BytebufType.VALID,     257,        -1,       0},
                {Position.INF,      SourceType.FILLED,      BytebufType.NULL,     257,          1,       0},
                {Position.INF,      SourceType.FILLED,      BytebufType.VALID,     257,          257,       0},

                {Position.INF,      SourceType.FILLED,      BytebufType.EMPTY,     0,          257,       0},
                {Position.INF,      SourceType.FILLED,      BytebufType.EMPTY,     0,          -1,       0},
                {Position.INF,      SourceType.FILLED,      BytebufType.NULL,     0,          1,       0},


                //added to improve jacoco coverage
                {Position.INF,      SourceType.FILLED,      BytebufType.VALID,     255,          1,      1},
                {Position.INF,      SourceType.FILLED,      BytebufType.VALID,     0,          1,      256},
                {Position.INF,      SourceType.FILLED,      BytebufType.NULL,      0,          1,        0},

                //added to improve ba-dua coverage and cover new test cases

                {Position.SUP,      SourceType.FILLED,     BytebufType.VALID,     0,          1,        256},
                {Position.SUP,      SourceType.EMPTY,      BytebufType.VALID,     255,          1,        0},
                {Position.SUP,      SourceType.NULL,       BytebufType.VALID,     255,          1,        0},
                {Position.SUP,      SourceType.NOT_VALID,  BytebufType.VALID,     255,          1,        0},

                //improve pit-test mutation score

                {Position.SUP,      SourceType.EMPTY,      BytebufType.VALID,     255,          1,        0},
                {Position.SUP,      SourceType.FILLED,     BytebufType.NULL,     0,          1,        0},
                {Position.SUP,      SourceType.EMPTY,      BytebufType.EMPTY,     -1,          -1,        0},


        });
    }

    private FileChannel getMockedFileChannel() throws IOException {
        FileChannel fc = mock(FileChannel.class);
        when(fc.read(isA(ByteBuffer.class),isA(Long.class))).thenThrow(new RuntimeException());
        return fc;
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
        catch(Exception e){
            Assert.assertTrue(this.expectedArgumentExc);
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

    private enum SourceType {
        EMPTY,
        FILLED,
        NOT_VALID,
        NULL
    }

    private enum Position{
        INF,
        SUP
    }



}
