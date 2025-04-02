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

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPException;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link FTPInputStream}, copied from Hadoop and modified, that doesn't throw an exception when seeks are attempted
 * to the current position. Position equality check logic in {@link FTPInputStream#seek} is the only change from the
 * original class in Hadoop. This change is required since {@link LineRecordReader} calls {@link FTPInputStream#seek}
 * with value of 0. TODO: This file can be removed once https://cdap.atlassian.net/browse/CDAP-5387 is addressed.
 */
public class FTPInputStream extends FSInputStream {

  public static final String SEEK_NOT_SUPPORTED = "Seek not supported";
  InputStream wrappedStream;
  FTPClient client;
  FileSystem.Statistics stats;
  boolean closed;
  long pos;

  public FTPInputStream(InputStream stream, FTPClient client,
                        FileSystem.Statistics stats) {
    if (stream == null) {
      throw new IllegalArgumentException("Null InputStream");
    }
    if (client == null || !client.isConnected()) {
      throw new IllegalArgumentException("FTP client null or not connected");
    }
    this.wrappedStream = stream;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  // We don't support seek unless the current position is same as the desired position.
  @Override
  public void seek(long pos) throws IOException {
    // If seek is to the current pos, then simply return. This logic was added so that the seek call in
    // LineRecordReader#initialize method to '0' does not fail.
    if (getPos() == pos) {
      return;
    }
    String errorMessage = "Operation not supported: Seek functionality is not available for this FTP server.";
    throw ErrorUtils.getProgramFailureException(
      new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage,
      errorMessage, ErrorType.USER, true, null);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    String errorMessage = "Operation not supported: Seek functionality is not available for this FTP server.";
    throw ErrorUtils.getProgramFailureException(
      new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage,
      errorMessage, ErrorType.USER, true, null);
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      String errorMessage = "Stream closed. Ensure that the stream is not closed before attempting" +
        " to read from it.";
      throw ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage,
        errorMessage, ErrorType.UNKNOWN, true, null);
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      String errorMessage = "Stream closed. Ensure that the stream is not closed before attempting" +
        " to read from it.";
      throw ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage,
        errorMessage, ErrorType.UNKNOWN, true, null);
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null && result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      String errorMessage = "The stream is already closed. Please ensure the stream is open before attempting to " +
        "close it again.";
      throw ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage,
        errorMessage, ErrorType.UNKNOWN, true, null);
    }
    super.close();
    closed = true;
    if (!client.isConnected()) {
      throw new FTPException("Client not connected");
    }

    boolean cmdCompleted = client.completePendingCommand();
    client.logout();
    client.disconnect();
    if (!cmdCompleted) {
      throw new FTPException("Could not complete transfer, Reply Code - "
                               + client.getReplyCode());
    }
  }

  // Not supported.

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @Override
  public void reset() throws IOException {
    String errorMessage = "Operation not supported: Mark functionality is not available for this FTP server.";
    throw ErrorUtils.getProgramFailureException(
      new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage,
      errorMessage, ErrorType.USER, true, null);
  }
}
