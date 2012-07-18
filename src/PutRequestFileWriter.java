
package org.hbase.async;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PutRequestFileWriter {
  void open();

  PutRequestFileWriter append(PutRequest rpc);

  void close();
}

class AsyncPutRequestFileWriter implements PutRequestFileWriter, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncPutRequestFileWriter.class);
  private final BlockingQueue<PutRequest> queue = new LinkedBlockingQueue<PutRequest>(256);
  private volatile boolean started = false;
  private ByteBuffer byteBuff = ByteBuffer.allocate((Integer.SIZE / Byte.SIZE));
  private String fileName;
  private OutputStream out;
  private long running_count_bytes;
  private long nextRollover;
  private Thread writer_thread;
  private long saved_events;
  private long rollindex;

  /**
   * The default maximum file size is 100MB.
   */
  private long maxFileSize = 100 * 1024 * 1024;

  public AsyncPutRequestFileWriter(String file) throws IOException {
    /*
     * When we start, it may be the case that some writes are pending to be
     * rolled up. Hence open the file in append mode.
     */
    this.setFile(file, true);
  }

  public PutRequestFileWriter append(PutRequest put) {
    if (!started) {
      throw new IllegalStateException("open() call expected before append()");
    }
    try {
      queue.put(put);
    } catch (InterruptedException e) {
      LOG.warn(e.getMessage());
    }
    return this;
  }

  public void open() {
    LOG.debug("Opening AsyncFileWriter.");
    this.started = true;
    this.writer_thread = new Thread(this);
    this.writer_thread.setName("AsyncWriter Thread");
    this.writer_thread.setDaemon(true);
    this.writer_thread.start();
  }

  public void run() {
    while (this.started) {
      try {
        dequeOneItem();
      } catch (InterruptedException e) {
        LOG.debug("Writer thread interrupted, exiting loop.");
        break;
      }
    }
    LOG.info("Writer has been stopped, flushing all pending items in queue before exiting, items to flush: "
        + queue.size());
    for (PutRequest p : queue) {
      try {
        write(p);
      } catch (IOException e) {
        LOG.warn("Error saving " + p + e.getMessage());
      }
    }
    try {
      closeFile();
    } catch (IOException e) {
      LOG.error("Error closing output file", e);
    }
  }

  private void dequeOneItem() throws InterruptedException {
    PutRequest item = queue.poll(100, TimeUnit.MICROSECONDS);
    if (item != null) {
      try {
        write(item);
      } catch (IOException e) {
        LOG.warn("Error saving " + item + e.getMessage());
      }
    }
  }

  public void write(PutRequest put) throws IOException {
    if (fileName != null && out != null) {
      long size = running_count_bytes;
      if (size >= getMaxFileSize() && size >= nextRollover) {
        LOG.debug("Size : " + size + " max file size " + getMaxFileSize() + " nextRollover "
            + nextRollover);
        rollOver();
      }
      ChannelBuffer buf = put.serializeWithTablename();
      byteBuff.putInt(buf.readableBytes());
      byteBuff.rewind();
      out.write(byteBuff.array());
      running_count_bytes += buf.readableBytes() + byteBuff.capacity();
      saved_events++;
      buf.readBytes(out, buf.readableBytes());
      byteBuff.clear();
    }
  }

  public void close() {
    if (!this.started)
      return;
    LOG.debug("Stopping AsyncFileWriter from mail thread.");
    this.writer_thread.interrupt();
    try {
      LOG.debug("Waiting for writer thread to join.");
      this.writer_thread.join(1000);
    } catch (InterruptedException e) {
      LOG.error("Failed to join worker thread.");
    }
    LOG.info("Total events saved in lifetime : " + saved_events);
    this.started = false;
  }

  private void setFile(String file, boolean append) throws IOException {
    LOG.debug("Set file called for " + file);
    reset();
    FileOutputStream ostream = null;
    try {
      ostream = new FileOutputStream(file, append);
    } catch (FileNotFoundException ex) {
      LOG.debug("FileNotFoundException for " + file + ", " +
          "attempting to create parent directory.");
      /*
       * if parent directory does not exist then attempt to create it and try to
       * create file
       */
      String parentName = new File(file).getParent();
      if (parentName != null) {
        File parentDir = new File(parentName);
        if (!parentDir.exists() && parentDir.mkdirs()) {
          ostream = new FileOutputStream(file, append);
        } else {
          throw ex;
        }
      } else {
        throw ex;
      }
    }
    this.fileName = file;
    this.out = new BufferedOutputStream(ostream);
    LOG.debug("setFile ended");
  }

  private void closeFile() throws IOException {
    if (this.out != null) {
      LOG.debug("Closing file " + fileName);
      out.close();
    }
  }

  private void reset() throws IOException {
    closeFile();
    this.fileName = null;
    this.out = null;
  }

  private void rollOver() throws IOException {
    LOG.debug("Roll over attempt started, events saved so far " + saved_events);
    boolean renameSucceeded = true;
    File target;
    long size = this.running_count_bytes;
    // if operation fails, do not roll again until
    // maxFileSize more bytes are written
    this.nextRollover = size + getMaxFileSize();
    File file;
    target = new File(this.fileName + "." + (++rollindex) + "." + System.currentTimeMillis());
    this.closeFile(); // close current file;
    file = new File(this.fileName);
    LOG.debug("Renaming file " + file + " to " + target);
    renameSucceeded = file.renameTo(target);
    if (!renameSucceeded) {
      this.setFile(this.fileName, true);
    }
    if (renameSucceeded) {
      this.setFile(this.fileName, false);
      this.nextRollover = 0;
      this.running_count_bytes = 0;
    }
    LOG.debug("Roll over attempt ended.");
  }

  public long getMaxFileSize() {
    return maxFileSize;
  }

  public void setMaxFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

}
