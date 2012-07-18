
package org.hbase.async;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPutFileWriter {

  @BeforeClass
  public static void oneTimeSetUp() {
    for (File file : getAllPutFiles(new File("/tmp"))) {
      file.delete();
    }
  }

  public static List<File> getAllPutFiles(File dir) {
    List<File> list = Arrays.asList(dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("async_test.bin");
      }
    }));
    return list;
  }

  static class PutGenerator {
    private final AtomicLong key = new AtomicLong();
    private byte[] t = new byte[] {
        't'
    };
    private byte[] f = new byte[] {
        'f'
    };
    private byte[] c = new byte[] {
        'c'
    };
    private byte[] v = new byte[] {
        'v'
    };

    public PutRequest newPut() {
      long key_l = key.incrementAndGet();
      return new PutRequest(t, Bytes.fromLong(key_l), f, c, v);
    }
  }

  private void test(final int threadCount) throws InterruptedException, ExecutionException,
      IOException {
    final PutGenerator putGenerator = new PutGenerator();
    final PutRequestFileWriter putWriter = new AsyncPutRequestFileWriter("/tmp/async_test.bin");
    putWriter.open();
    ((AsyncPutRequestFileWriter) putWriter).setMaxFileSize(10000); //10k(ish..)
    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() {
        for (int i = 0; i < 100; i++)
          putWriter.append(putGenerator.newPut());
        return null;
      }
    };
    List<Callable<Void>> tasks = Collections.nCopies(threadCount, task);
    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    List<Future<Void>> futures = executorService.invokeAll(tasks);
    for (Future<Void> future : futures) {
      future.get();
    }
    putWriter.close();

    Assert.assertEquals(futures.size(), threadCount);

    List<Long> expectedList = new ArrayList<Long>(threadCount * 100);
    List<Long> resultList = new ArrayList<Long>(futures.size() * 100);
    for (long i = 1; i <= threadCount * 100; i++) {
      expectedList.add(i);
    }
    for (File filename : getAllPutFiles(new File("/tmp"))) {
      PutRequestFileReader putfile = new PutRequestFileReader(filename.getAbsolutePath());
      for (PutRequest p : putfile) {
        resultList.add(Bytes.getLong(p.key()));
      }
    }
    Collections.sort(resultList);
    System.out.println(resultList.size());
    System.out.println(resultList);
    Assert.assertEquals(expectedList, resultList);

  }

  @Test
  public void test2() throws InterruptedException, ExecutionException, IOException {
    test(2);
  }

  @AfterClass
  public static void oneTimeTearDown() {
    for (File file : getAllPutFiles(new File("/tmp"))) {
      file.delete();
    }
  }
}
