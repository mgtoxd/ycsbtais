/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import site.ycsb.*;
import site.ycsb.db.prot.CacheGrpc;
import site.ycsb.db.prot.CloudGrpc;
import site.ycsb.db.prot.File;
import site.ycsb.db.prot.Hash;

import java.util.*;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class TaisClient extends DB {


  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String TARGET_PROPERTY = "tais.target";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  public static String target;
  public static long[] hashlist;

  public void init() throws DBException {
    System.out.println("初始化");
    Properties props = getProperties();
    target = props.getProperty(TARGET_PROPERTY);
    hashlist = new long[]{-7498865567819644540L, 833109335530526850L, 7895190270884542097L, -4806962370485529156L, -4421892272683488087L,
        -7059059793295010438L, -6586589624490999927L, -1539937758087190957L, 3544895871231895457L, -7581044683199380110L, 3794417534135048625L,
        -4331079542901015323L, 2219461923712936128L, -8538794395895381351L, 1615261191024161232L, -3100467696533447125L, -5074045478793549103L};
  }

  public void cleanup() throws DBException {

  }


  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    System.out.println(key);
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).maxInboundMessageSize(50 * 1024 * 1024).usePlaintext().build();
    CacheGrpc.CacheBlockingStub blockingStub = CacheGrpc.newBlockingStub(channel);
    System.out.println("key:" + key);
    File file = blockingStub.getFile(Hash.newBuilder().setHash(hashlist[Integer.parseInt(key) - 1]).build());
    System.out.println(file.getName());
    channel.shutdownNow();
    return Status.BATCHED_OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return Status.BATCHED_OK;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return Status.BATCHED_OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.BATCHED_OK;
  }

}
