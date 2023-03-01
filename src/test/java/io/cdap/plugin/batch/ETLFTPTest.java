/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.batch;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.batch.source.ftp.FTPBatchSource;
import io.cdap.plugin.batch.source.ftp.FTPLocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * FTP Source Test.
 */
public class ETLFTPTest extends ETLBatchTestBase {
  private static final String TEST_STRING = "Hello World";
  private static final String TEST_STRING_2 = "Goodnight Moon";
  private static File folder;
  private static File file2;
  private static FakeFtpServer ftpServer;
  private static final String IGNORE_NON_EXISTING_FOLDERS = "ignoreNonExistingFolders";
  private static final String FILE_REGEX = "fileRegex";

  @Before
  public void setup() throws IOException {
    folder = TMP_FOLDER.newFolder();
  }

  @After
  public void stop() {
    if (ftpServer != null) {
      ftpServer.stop();
    }
  }

  /**
   * Start a fake FTP Server with a couple fake text files and return the URI for the base folder.
   */
  private FTPLocation startFtpServer() throws Exception {
    File file = new File(folder, "sample");
    file2 = new File(folder, "sample2");
    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(file.getAbsolutePath(), TEST_STRING));
    fileSystem.add(new FileEntry(file2.getAbsolutePath(), TEST_STRING_2));
    return startFtpServer(fileSystem);
  }

  /**
   * Start a fake FTP Server with the given filesystem and return the URI for the base folder
   */
  private FTPLocation startFtpServer(FileSystem fileSystem) throws Exception {
    String user = "ftp";
    String pwd = "abcd";
    return startFtpServer(fileSystem, user, pwd);
  }

  /**
   * Start a fake FTP Server with the given filesystem and return the URI for the base folder
   */
  private FTPLocation startFtpServer(FileSystem fileSystem, String user, String pwd) throws Exception {
    ftpServer = new FakeFtpServer();
    ftpServer.setServerControlPort(0);
    ftpServer.setFileSystem(fileSystem);
    ftpServer.addUserAccount(new UserAccount(user, pwd, folder.getAbsolutePath()));
    ftpServer.start();

    Tasks.waitFor(true, () -> ftpServer.isStarted(), 5, TimeUnit.SECONDS);
    int port = ftpServer.getServerControlPort();
    URI uri = new URI("ftp", null, "localhost", port, folder.getAbsolutePath(), null, null);
    return new FTPLocation(FTPLocation.Type.FTP, uri, user, pwd);
  }

  @Test
  public void testText() throws Exception {
    FTPLocation location = startFtpServer();
    Map<String, String> properties = getLocationProperties(location);
    properties.put(IGNORE_NON_EXISTING_FOLDERS, "false");
    properties.put("referenceName", "ftp");
    properties.put("format", "text");
    ETLStage source = new ETLStage("source", new ETLPlugin(FTPBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    List<StructuredRecord> output = runPipeline(source);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains(TEST_STRING));
    Assert.assertTrue(outputValue.contains(TEST_STRING_2));
  }

  @Test
  public void testBlob() throws Exception {
    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(folder.getAbsolutePath() + "/test.txt", "a\nb\nc"));
    FTPLocation location = startFtpServer(fileSystem);
    Map<String, String> properties = getLocationProperties(location);
    properties.put("referenceName", "ftp");
    properties.put("format", "blob");
    ETLStage source = new ETLStage("source", new ETLPlugin(FTPBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    List<StructuredRecord> output = runPipeline(source);
    Assert.assertEquals(1, output.size());
    ByteBuffer bodyBytes = output.get(0).get("body");
    Assert.assertArrayEquals("a\nb\nc".getBytes(StandardCharsets.UTF_8), Bytes.toBytes(bodyBytes));
  }

  @Test
  public void testJson() throws Exception {
    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(folder.getAbsolutePath() + "/test.json", "{ \"a\":\"x\" }"));
    FTPLocation location = startFtpServer(fileSystem);

    Schema schema = Schema.recordOf("r",
                                    Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Map<String, String> properties = getLocationProperties(location);
    properties.put("referenceName", "ftp");
    properties.put("format", "json");
    properties.put("schema", schema.toString());
    ETLStage source = new ETLStage("source", new ETLPlugin(FTPBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    List<StructuredRecord> output = runPipeline(source);
    List<StructuredRecord> expected = Collections.singletonList(
      StructuredRecord.builder(schema).set("a", "x").build());
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testCSV() throws Exception {
    testDelimited("csv", ",", true);
    testDelimited("csv", ",", false);
  }

  @Test
  public void testTSV() throws Exception {
    testDelimited("tsv", "\t", true);
    testDelimited("tsv", "\t", false);
  }

  @Test
  public void testDelimited() throws Exception {
    testDelimited("delimited", "|", true);
    testDelimited("delimited", "|", false);
  }

  private void testDelimited(String format, String delimiter, boolean useHeader) throws Exception {
    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = new HashSet<>(2);
    expected.add(StructuredRecord.builder(schema)
                   .set("id", 0).set("name", "Alice").build());
    expected.add(StructuredRecord.builder(schema)
                   .set("id", 1).set("name", "Bob").set("email", "bob@example.com").build());
    FileSystem fileSystem = new UnixFakeFileSystem();

    StringBuilder fileContent = new StringBuilder();
    if (useHeader) {
      fileContent.append("id").append(delimiter).append("name").append(delimiter).append("email").append("\n");
    }
    fileContent.append("0").append(delimiter).append("Alice").append(delimiter).append("\n");
    fileContent.append("1").append(delimiter).append("Bob").append(delimiter).append("bob@example.com").append("\n");
    fileSystem.add(new FileEntry(folder.getAbsolutePath() + "/test.csv", fileContent.toString()));
    FTPLocation location = startFtpServer(fileSystem);
    Map<String, String> properties = getLocationProperties(location);
    properties.put("referenceName", "ftp");
    properties.put("format", format);
    properties.put("delimiter", delimiter);
    properties.put("skipHeader", Boolean.valueOf(useHeader).toString());
    properties.put("schema", schema.toString());
    ETLStage source = new ETLStage("source", new ETLPlugin(FTPBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    Set<StructuredRecord> output = new HashSet<>(runPipeline(source));
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testFTPBatchSourceWithRegex() throws Exception {
    FTPLocation location = startFtpServer();
    Map<String, String> properties = getLocationProperties(location);
    properties.put("referenceName", "ftp");
    properties.put(IGNORE_NON_EXISTING_FOLDERS, "false");
    properties.put(FILE_REGEX, file2.getAbsolutePath());
    properties.put("format", "text");
    ETLStage source = new ETLStage("source", new ETLPlugin(FTPBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    List<StructuredRecord> output = runPipeline(source);

    Assert.assertEquals("Expected records", 1, output.size());
    Assert.assertEquals("Single file", TEST_STRING_2, output.get(0).get("body"));
  }

  @Test
  public void testFTPBatchSourceWithMacro() throws Exception {
    FTPLocation location = startFtpServer();
    Map<String, String> properties = new HashMap<>();
    properties.put("referenceName", "${referenceName}");
    properties.put("host", "${host}");
    properties.put("port", "${port}");
    properties.put("path", "${path}");
    properties.put("user", "${user}");
    properties.put("type", "${type}");
    properties.put("password", "${password}");
    properties.put("format", "${format}");
    ETLStage source = new ETLStage("source", new ETLPlugin(FTPBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));

    Map<String, String> args = getLocationProperties(location);
    args.put("referenceName", "ftp_with_macro");
    args.put("format", "text");
    List<StructuredRecord> output = runPipeline(source, args);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello World"));
  }

  private Map<String, String> getLocationProperties(FTPLocation location) {
    Map<String, String> properties = new HashMap<>();
    URI uri = location.getURI();
    properties.put("host", uri.getHost());
    properties.put("type", uri.getScheme());
    properties.put("path", uri.getPath());
    properties.put("port", String.valueOf(uri.getPort()));
    properties.put("user", location.getUser());
    properties.put("password", location.getPassword());
    return properties;
  }

  @Test
  public void testUsernameWithColon() throws Exception {
    File file = new File(folder, "sample");
    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(file.getAbsolutePath(), TEST_STRING));
    FTPLocation location = startFtpServer(fileSystem, "username::::@&1", "abc:::@!/");
    Map<String, String> properties = getLocationProperties(location);
    properties.put(IGNORE_NON_EXISTING_FOLDERS, "false");
    properties.put("referenceName", "ftp");
    properties.put("format", "text");
    ETLStage source = new ETLStage("source", new ETLPlugin(FTPBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
    List<StructuredRecord> output = runPipeline(source);

    Assert.assertEquals("Expected records", 1, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains(TEST_STRING));
  }

  private List<StructuredRecord> runPipeline(ETLStage sourceConfig) throws Exception {
    return runPipeline(sourceConfig, Collections.emptyMap());
  }

  private List<StructuredRecord> runPipeline(ETLStage sourceConfig, Map<String, String> runtimeArgs) throws Exception {
    String outputDatasetName = UUID.randomUUID().toString();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(sourceConfig)
      .addStage(sink)
      .addConnection(sourceConfig.getName(), sink.getName())
      .setEngine(Engine.SPARK)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());

    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(runtimeArgs, ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }
}
