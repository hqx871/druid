/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexer;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.IOUtils;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HdfsClasspathSetupTest
{
  private static FileSystem localFS;
  private static File hdfsTmpDir;
  private static Configuration conf;
  private static String dummyJarString = "This is a test jar file.";
  private File dummyJarFile;
  private Path finalClasspath;
  private Path intermediatePath;
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupStatic() throws IOException
  {
    hdfsTmpDir = File.createTempFile("hdfsClasspathSetupTest", "dir");
    if (!hdfsTmpDir.delete()) {
      throw new IOE("Unable to delete hdfsTmpDir [%s]", hdfsTmpDir.getAbsolutePath());
    }
    conf = new Configuration(true);
    localFS = new LocalFileSystem();
    localFS.initialize(hdfsTmpDir.toURI(), conf);
  }

  @Before
  public void setUp() throws IOException
  {
    // intermedatePath and finalClasspath are relative to hdfsTmpDir directory.
    intermediatePath = new Path(StringUtils.format("/tmp/classpath/%s", UUIDUtils.generateUuid()));
    finalClasspath = new Path(StringUtils.format("/tmp/intermediate/%s", UUIDUtils.generateUuid()));
    dummyJarFile = tempFolder.newFile("dummy-test.jar");
    Files.copy(
        new ByteArrayInputStream(StringUtils.toUtf8(dummyJarString)),
        dummyJarFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING
    );
  }

  @AfterClass
  public static void tearDownStatic() throws IOException
  {
    FileUtils.deleteDirectory(hdfsTmpDir);
  }

  @After
  public void tearDown() throws IOException
  {
    dummyJarFile.delete();
    Assert.assertFalse(dummyJarFile.exists());
    localFS.delete(finalClasspath, true);
    Assert.assertFalse(localFS.exists(finalClasspath));
    localFS.delete(intermediatePath, true);
    Assert.assertFalse(localFS.exists(intermediatePath));
  }

  @Test
  public void testAddSnapshotJarToClasspath() throws IOException
  {
    Job job = Job.getInstance(conf, "test-job");
    Path intermediatePath = new Path("/tmp/classpath");
    JobHelper.addSnapshotJarToClassPath(dummyJarFile, intermediatePath, localFS, job);
    Path expectedJarPath = new Path(intermediatePath, dummyJarFile.getName());
    // check file gets uploaded to HDFS
    Assert.assertTrue(localFS.exists(expectedJarPath));
    // check file gets added to the classpath
    Assert.assertEquals(expectedJarPath.toString(), job.getConfiguration().get(MRJobConfig.CLASSPATH_FILES));
    Assert.assertEquals(dummyJarString, StringUtils.fromUtf8(IOUtils.toByteArray(localFS.open(expectedJarPath))));
  }

  @Test
  public void testAddNonSnapshotJarToClasspath() throws IOException
  {
    Job job = Job.getInstance(conf, "test-job");
    JobHelper.addJarToClassPath(dummyJarFile, finalClasspath, intermediatePath, localFS, job);
    Path expectedJarPath = new Path(finalClasspath, dummyJarFile.getName());
    // check file gets uploaded to final HDFS path
    Assert.assertTrue(localFS.exists(expectedJarPath));
    // check that the intermediate file gets deleted
    Assert.assertFalse(localFS.exists(new Path(intermediatePath, dummyJarFile.getName())));
    // check file gets added to the classpath
    Assert.assertEquals(expectedJarPath.toString(), job.getConfiguration().get(MRJobConfig.CLASSPATH_FILES));
    Assert.assertEquals(dummyJarString, StringUtils.fromUtf8(IOUtils.toByteArray(localFS.open(expectedJarPath))));
  }

  @Test
  public void testIsSnapshot()
  {
    Assert.assertTrue(JobHelper.isSnapshot(new File("test-SNAPSHOT.jar")));
    Assert.assertTrue(JobHelper.isSnapshot(new File("test-SNAPSHOT-selfcontained.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("test.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("test-selfcontained.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("iAmNotSNAPSHOT.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("iAmNotSNAPSHOT-selfcontained.jar")));

  }

  @Test
  public void testConcurrentUpload() throws InterruptedException, ExecutionException, TimeoutException
  {
    final int concurrency = 10;
    ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(concurrency));
    // barrier ensures that all jobs try to add files to classpath at same time.
    final CyclicBarrier barrier = new CyclicBarrier(concurrency);
    final Path expectedJarPath = new Path(finalClasspath, dummyJarFile.getName());
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();

    for (int i = 0; i < concurrency; i++) {
      futures.add(
          pool.submit(
              new Callable()
              {
                @Override
                public Boolean call() throws Exception
                {
                  int id = barrier.await();
                  Job job = Job.getInstance(conf, "test-job-" + id);
                  Path intermediatePathForJob = new Path(intermediatePath, "job-" + id);
                  JobHelper.addJarToClassPath(dummyJarFile, finalClasspath, intermediatePathForJob, localFS, job);
                  // check file gets uploaded to final HDFS path
                  Assert.assertTrue(localFS.exists(expectedJarPath));
                  // check that the intermediate file is not present
                  Assert.assertFalse(localFS.exists(new Path(intermediatePathForJob, dummyJarFile.getName())));
                  // check file gets added to the classpath
                  Assert.assertEquals(
                      expectedJarPath.toString(),
                      job.getConfiguration().get(MRJobConfig.CLASSPATH_FILES)
                  );
                  return true;
                }
              }
          )
      );
    }

    Futures.allAsList(futures).get(30, TimeUnit.SECONDS);

    pool.shutdownNow();
  }

}
