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
package io.cdap.plugin.batch.source.ftp;


import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.format.delimited.input.CSVInputFormatProvider;
import io.cdap.plugin.format.delimited.input.DelimitedConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit test for {@link FTPBatchSource} class.
 */
public class FTPBatchSourceTest {
  private static final String USER_WITH_SPECIAL_CHARACTERS = "user/12^!@_!";
  private static final String PASSWORD_WITH_SPECIAL_CHARACTERS = "we:/%x^Yz@123#456!";
  private static final String HOST = "localhost";
  private static FakeFtpServer ftpServer;
  private static int ftpServerPort;

  @BeforeClass
  public static void ftpSetup() {
    ftpServer = new FakeFtpServer();
    ftpServer.addUserAccount(new UserAccount(USER_WITH_SPECIAL_CHARACTERS, PASSWORD_WITH_SPECIAL_CHARACTERS, "/tmp"));
    ftpServer.setServerControlPort(0);

    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new DirectoryEntry("/tmp"));
    fileSystem.add(new FileEntry("/tmp/file1.txt", "hello,world"));
    fileSystem.add(new FileEntry("/tmp/file2.txt", "hello world"));
    fileSystem.add(new FileEntry("/tmp/file3.txt", "hello world"));

    ftpServer.setFileSystem(fileSystem);
    ftpServer.start();
    ftpServerPort = ftpServer.getServerControlPort();
  }

  @AfterClass
  public static void ftpTeardown() {
    ftpServer.stop();
  }

  @Test
  public void testSchemaDetection() {
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.FTP)
      .setHost(HOST)
      .setPort(ftpServerPort)
      .setPath("/tmp/file1.txt")
      .setUser(USER_WITH_SPECIAL_CHARACTERS)
      .setPassword(PASSWORD_WITH_SPECIAL_CHARACTERS)
      .setFormat("csv")
      .build();
    FTPBatchSource ftpBatchSource = new FTPBatchSource(config);
    Map<String, Object> plugins = new HashMap<>();
    plugins.put("csv", new CSVInputFormatProvider(new DelimitedConfig()));
    MockPipelineConfigurer pipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    ftpBatchSource.configurePipeline(pipelineConfigurer);
    Schema outputSchema = pipelineConfigurer.getOutputSchema();
    Schema expectedSchema = Schema.recordOf("text",
                                            Schema.Field.of("body_0", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("body_1", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expectedSchema, outputSchema);
  }
}


