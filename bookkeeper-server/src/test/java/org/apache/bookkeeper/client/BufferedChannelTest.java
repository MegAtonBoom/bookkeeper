package org.apache.bookkeeper.client;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
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
    private BufferedChannel bc;

    //method params
    private ByteBuf destBuf;
    private int pos;
    private int length;

    //expected
    private int exp;

    private boolean expectedIllegalArgument;
    private boolean expectedNullPointer;
    private boolean expectedIO;


    public BufferedChannelTest(BytebufType bbt, int pos, int length, int exp) throws IOException {

        configure(bbt, pos, length, exp);

    }

    private void configure(BytebufType bbt, int pos, int length, int exp) throws IOException {

        if(bbt==BytebufType.NULL) this.expectedNullPointer = true;
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

        this.tmpFile = File.createTempFile("file", "log");
        this.tmpFile.deleteOnExit();

        this.fc = new RandomAccessFile(tmpFile, "rw").getChannel();

        this.bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fc, this.buffSize*2, 0);

        this.bc.write(writeBuf);

    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][]{
                //ByteBuf type          pos         length     expected return
                {BytebufType.NULL,     -1,          5,         0},
                {BytebufType.VALID,     257,        -10,       0},
                {BytebufType.EMPTY,     0,          257,       0},

                //added to improve jacoco coverage
                {BytebufType.VALID,     231,          25,      25},
                {BytebufType.NULL,      0,          25,        0}

        });
    }


    @Test
    public void testRead(){
        try{

            BookKeeper bk = new BookKeeper("servers");
            int copied=this.bc.read(this.destBuf, this.pos, this.length);
            System.out.println("no exc "+ copied);
            Assert.assertEquals(copied, this.exp);
        }
        catch(NullPointerException ne){
            System.out.println("ne");
            Assert.assertTrue(this.expectedNullPointer);
        }
        catch(IOException ioe){
            System.out.println("ioe");
            Assert.assertTrue(this.expectedIO);
        }
        catch(IllegalArgumentException ie){
            System.out.println("ie");
            Assert.assertTrue(this.expectedIllegalArgument);
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }


    private enum BytebufType{
        VALID,
        NULL,
        EMPTY;
    }



}
