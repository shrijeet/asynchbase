
package org.hbase.async;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class PutRequestFileReader implements Iterable<PutRequest> {
  public String filename;

  public PutRequestFileReader(String filename) {
    this.filename = filename;
  }

  public Iterator<PutRequest> iterator() {
    return new PutFileIterator();
  }

  class PutFileIterator implements Iterator<PutRequest> {

    DataInputStream in;
    PutRequest nextput;

    public PutFileIterator() {
      try {
        in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
        nextput = readOnePut(in);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    private PutRequest readOnePut(DataInputStream in) throws IOException {
      try {
        int len = in.readInt();
        byte[] arr = new byte[len];
        in.read(arr, 0, len);
        ChannelBuffer buf = ChannelBuffers.copiedBuffer(arr);
        PutRequest new_put = PutRequest.deserializeFrom(buf);
        return new_put;
      } catch (EOFException e) {
        return null;
      }
    }

    @Override
    public boolean hasNext() {
      return nextput != null;
    }

    @Override
    public PutRequest next() {
      try {
        PutRequest ret = nextput;
        if (nextput != null) {
          nextput = readOnePut(in);
          if (nextput == null) {
            in.close();
          }
        }
        return ret;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported for read only put file.");
    }

  }

  public static void main(String[] args) throws InterruptedException {
    File dir = new File(args[0]);
    File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("asyncputs.bin");
      }
    });
    for (File filename : files) {
      System.out.println("Reading " + filename);
      PutRequestFileReader putfile = new PutRequestFileReader(filename.getAbsolutePath());
      for (PutRequest p : putfile) {
        System.out.println(p);
      }
    }

  }
}
