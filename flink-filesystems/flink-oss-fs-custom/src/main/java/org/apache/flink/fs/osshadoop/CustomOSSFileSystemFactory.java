/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.osshadoop;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.flink.core.fs.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.Constants;

public class CustomOSSFileSystemFactory extends OSSFileSystemFactory {
    private static final String CUSTOM_OSS_SCHEME = "custom-oss";

    private Configuration config;

    @Override
    public String getScheme() {
        return CUSTOM_OSS_SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        this.config = new Configuration();
        URI uri = parse(fsUri, this.config);

        return super.create(uri);
    }

    @Override
    public Configuration getHadoopConfiguration() {
        Configuration hadoopConfig = super.getHadoopConfiguration();

        this.config.forEach(e -> hadoopConfig.set(e.getKey(), e.getValue()));

        return hadoopConfig;
    }

    /* The origUri format must be:
     *   custom-oss://{keyId}:{keySecret}@{bucket}.{endpoint}/{filePath}
     */
    private static URI parse(URI origUri, Configuration conf) {
        String userInfo = origUri.getUserInfo();
        String[] userInfoSegments = origUri.getUserInfo().split(":");
        Preconditions.checkArgument(userInfoSegments.length == 2, "Illegal userInfo format: " + userInfo);

        String accessKeyId = userInfoSegments[0];
        String accessKeySecret = userInfoSegments[1];

        String host = origUri.getHost();
        String[] hostSegments = host.split("\\.", 2);
        Preconditions.checkArgument(hostSegments.length == 2, "Illegal host format: " + host);

        String bucket = hostSegments[0];
        String endpoint = hostSegments[1];

        conf.set(Constants.ENDPOINT_KEY, endpoint);
        conf.set(Constants.ACCESS_KEY_ID, accessKeyId);
        conf.set(Constants.ACCESS_KEY_SECRET, accessKeySecret);
        conf.set(Constants.CREDENTIALS_PROVIDER_KEY, AliyunCredentialsProvider.class.getName());

        try {
            return new URI(Constants.FS_OSS, bucket, origUri.getPath(), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
