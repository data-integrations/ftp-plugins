/*
 * Copyright © 2023 Cask Data, Inc.
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


import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link FTPConfig} class.
 */
public class FTPConfigTest {
  private static final String USER = "user";
  private static final String USER2 = "user:/@^#!%2";
  private static final String PASSWORD_WITH_SPECIAL_CHARACTERS = "we:/%x^Yz@123#456!";
  private static final String PASSWORD_WITHOUT_SPECIAL_CHARACTERS = "wexYz123456";
  private static final String HOST = "localhost";
  private static final int FTP_DEFAULT_PORT = 21;
  private static final int SFTP_DEFAULT_PORT = 22;
  private static final String PATH = "/user-look-here.txt";
  private static final String SFTP_PREFIX = "sftp";
  private static FakeFtpServer ftpServer;
  private static int ftpServerPort;

  @BeforeClass
  public static void ftpSetup() {
    ftpServer = new FakeFtpServer();
    ftpServer.addUserAccount(new UserAccount(USER, PASSWORD_WITHOUT_SPECIAL_CHARACTERS, "/tmp"));
    ftpServer.addUserAccount(new UserAccount(USER2, PASSWORD_WITH_SPECIAL_CHARACTERS, "/tmp"));
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
  public void testInvalidHost() {
    MockFailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.FTP)
      .setHost("invalid_host")
      .setPort(21)
      .setPath("/some/path")
      .setUser("user")
      .setPassword("password")
      .build();

    Assert.assertNull(config.getLocation(collector));
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
    List<ValidationFailure.Cause> causes = failures.get(0).getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("host", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testInvalidPort() {
    MockFailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.FTP)
      .setHost("example.com")
      .setPort(-5)
      .setPath("/some/path")
      .setUser("user")
      .setPassword("password")
      .build();

    Assert.assertNull(config.getLocation(collector));
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
    List<ValidationFailure.Cause> causes = failures.get(0).getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("port", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testFTPPathWithSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.FTP)
      .setHost(HOST)
      .setPort(FTP_DEFAULT_PORT)
      .setPath(PATH)
      .setUser(USER2)
      .setPassword(PASSWORD_WITH_SPECIAL_CHARACTERS)
      .build();
    config.validate(collector);

    FTPLocation location = config.getLocation(collector);
    HashMap<String, String> fileSystemProperties = new HashMap<>();
    fileSystemProperties.put("fs.ftp.impl", FTPFileSystem.class.getName());
    fileSystemProperties.put("fs.ftp.host", HOST);
    fileSystemProperties.put(String.format("fs.ftp.user.%s", HOST), USER2);
    fileSystemProperties.put("fs.ftp.host.port", String.valueOf(FTP_DEFAULT_PORT));
    fileSystemProperties.put(String.format("fs.ftp.password.%s", HOST), PASSWORD_WITH_SPECIAL_CHARACTERS);
    fileSystemProperties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
    Assert.assertEquals(fileSystemProperties, location.getHadoopProperties());
  }

  @Test
  public void testFTPPathWithoutSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.FTP)
      .setHost(HOST)
      .setPort(FTP_DEFAULT_PORT)
      .setPath(PATH)
      .setUser(USER)
      .setPassword(PASSWORD_WITHOUT_SPECIAL_CHARACTERS)
      .build();
    config.validate(collector);
  }

  @Test
  public void testSFTPPathWithSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.SFTP)
      .setHost(HOST)
      .setPort(SFTP_DEFAULT_PORT)
      .setPath(PATH)
      .setUser(USER2)
      .setPassword(PASSWORD_WITH_SPECIAL_CHARACTERS)
      .build();
    config.validate(collector);

    Map<String, String> fileSystemProperties = new HashMap<>();
    fileSystemProperties.put("fs.sftp.host", HOST);
    fileSystemProperties.put(String.format("fs.sftp.user.%s", HOST), USER2);
    fileSystemProperties.put("fs.sftp.host.port", String.valueOf(SFTP_DEFAULT_PORT));
    fileSystemProperties.put(String.format("fs.sftp.password.%s.%s", HOST, USER2), PASSWORD_WITH_SPECIAL_CHARACTERS);
    fileSystemProperties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
    fileSystemProperties.put("fs.sftp.impl", SFTPFileSystem.class.getName());
    fileSystemProperties.put("fs.sftp.impl.disable.cache", "true");
    FTPLocation location = config.getLocation(collector);
    Assert.assertEquals(fileSystemProperties, location.getHadoopProperties());
    Assert.assertEquals(String.format("%s://%s:%d%s", SFTP_PREFIX, HOST, SFTP_DEFAULT_PORT, PATH),
                        location.getURI().toString());
  }

  @Test
  public void testSFTPPathWithoutSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.SFTP)
      .setHost(HOST)
      .setPort(SFTP_DEFAULT_PORT)
      .setPath(PATH)
      .setUser(USER)
      .setPassword(PASSWORD_WITHOUT_SPECIAL_CHARACTERS)
      .build();
    config.validate(collector);
  }

  @Test
  public void testInvalidUsernamePasswordFTPPathConnection() {
    FailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.SFTP)
      .setHost(HOST)
      .setPort(SFTP_DEFAULT_PORT)
      .setPath(PATH)
      .setUser("wrong_user")
      .setPassword("wrong_password")
      .build();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testValidFTPPathConnection() {
    FailureCollector collector = new MockFailureCollector();
    FTPConfig config = FTPConfig.builder()
      .setType(FTPLocation.Type.FTP)
      .setHost(HOST)
      .setPort(ftpServerPort)
      .setPath("/")
      .setUser(USER)
      .setPassword(PASSWORD_WITHOUT_SPECIAL_CHARACTERS)
      .build();
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }
}


