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

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * The location of a (S)FTP directory or file, with utility methods to get the relevant Hadoop properties
 * needed to read from that location.
 */
public class FTPLocation {
  private final Type type;
  private final URI uri;
  private final int port;
  private final String user;
  private final String password;

  public FTPLocation(Type type, URI uri, String user, String password) {
    if (!type.scheme.equals(uri.getScheme())) {
      // should never happen
      throw new IllegalStateException(
        String.format("Server type %s and URI scheme %s do not match. " +
                        "This indicates a bug in the plugin, please contact support.",
                      type.scheme, uri.getScheme()));
    }
    this.type = type;
    this.uri = uri;
    this.user = user;
    this.password = password;
    int port = uri.getPort();
    this.port = port == -1 ? type.defaultPort : port;
  }

  public URI getURI() {
    return uri;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public Map<String, String> getHadoopProperties() {
    Map<String, String> properties = new HashMap<>();
    String host = uri.getHost();
    properties.put(String.format("fs.%s.host", type.scheme), host);
    properties.put(String.format("fs.%s.user.%s", type.scheme, host), user);
    properties.put(String.format("fs.%s.host.port", type.scheme), String.valueOf(port));
    switch (type) {
      case FTP:
        // password for ftp and sftp are specified slightly differently...
        properties.put(String.format("fs.%s.password.%s", type.scheme, host), password);
        break;
      case SFTP:
        properties.put("fs.sftp.impl", SFTPFileSystem.class.getName());
        properties.put("fs.ftp.impl.disable.cache", "true");
        properties.put(String.format("fs.%s.password.%s.%s", type.scheme, host, user), password);
        break;
      default:
        // should never happen
        throw new IllegalStateException("Unknown FTP type " + type);
    }
    // Limit the number of splits to 1 since FTPInputStream does not support seek;
    properties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
    return properties;
  }

  /**
   * Whether it is FTP or SFTP
   */
  public enum Type {
    FTP("ftp", 21),
    SFTP("sftp", 22);

    private final String scheme;
    private final int defaultPort;

    Type(String scheme, int defaultPort) {
      this.scheme = scheme;
      this.defaultPort = defaultPort;
    }

    public String getScheme() {
      return scheme;
    }

    public int getDefaultPort() {
      return defaultPort;
    }
  }
}
