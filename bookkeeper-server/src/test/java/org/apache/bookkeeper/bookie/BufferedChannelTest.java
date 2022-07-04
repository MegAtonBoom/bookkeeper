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
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;


@RunWith(Parameterized.class)
public class BufferedChannelTest {

    private ByteBuf writeBuf;
    private int buffSize=256;
    private File tmpFile;;
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


    public BufferedChannelTest(BytebufType bbt, int pos, int length, int exp) throws IOException {

        configure(bbt, pos, length, exp);

    }

    private void configure(BytebufType bbt, int pos, int length, int exp) throws IOException {

        if(bbt==BytebufType.NULL) this.expectedNullPointer = true;
        if(pos<0 || pos>this.buffSize) this.expectedArgumentExc = true;
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

        File currentDir = new File(".\\");
        this.tmpFile = new File(currentDir+"\\"+"test.txt");

        FileWriter Writer = new FileWriter(this.tmpFile);
        Writer.write("");
        Writer.close();

        this.fc = new RandomAccessFile(tmpFile, "rw").getChannel();

        this.bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fc, this.buffSize*2, 0);

        this.bc.write(writeBuf);

    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][]{
                //ByteBuf type          pos         length     expected return
                {BytebufType.NULL,     -1,          1,         0},
                {BytebufType.VALID,     -1,         -1,       0},
                {BytebufType.EMPTY,     -1,          257,       0},

                {BytebufType.VALID,     257,        -1,       0},
                {BytebufType.NULL,     257,          1,       0},
                {BytebufType.VALID,     257,          257,       0},

                {BytebufType.EMPTY,     0,          257,       0},
                {BytebufType.EMPTY,     0,          -1,       0},
                {BytebufType.NULL,     0,          1,       0},


                //added to improve jacoco coverage
                {BytebufType.VALID,     255,          1,      1},
                //{BytebufType.VALID,     0,          1,      1},
                {BytebufType.NULL,      0,          1,        0}



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
        catch(Exception e) {
            Assert.assertTrue(this.expectedArgumentExc);
        }

    }


    private enum BytebufType{
        VALID,
        NULL,
        EMPTY;
    }

    @After
    public void tearDown(){
        this.tmpFile.deleteOnExit();
    }



}
