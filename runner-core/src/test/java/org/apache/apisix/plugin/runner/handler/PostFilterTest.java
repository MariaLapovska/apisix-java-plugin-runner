/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.apisix.plugin.runner.handler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.flatbuffers.FlatBufferBuilder;
import io.github.api7.A6.TextEntry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.apisix.plugin.runner.A6Conf;
import org.apache.apisix.plugin.runner.A6ConfigRequest;
import org.apache.apisix.plugin.runner.A6ConfigResponse;
import org.apache.apisix.plugin.runner.A6ConfigWatcher;
import org.apache.apisix.plugin.runner.ExtraInfoRequest;
import org.apache.apisix.plugin.runner.PostRequest;
import org.apache.apisix.plugin.runner.PostResponse;
import org.apache.apisix.plugin.runner.filter.PluginFilter;
import org.apache.apisix.plugin.runner.filter.PluginFilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@DisplayName("test response from upstream")
public class PostFilterTest {
    private PrintStream console = null;
    private ByteArrayOutputStream bytes = null;

    RpcCallHandler rpcCallHandler;

    Cache<Long, A6Conf> cache;

    Map<String, PluginFilter> filters;

    List<A6ConfigWatcher> watchers;

    EmbeddedChannel channel;

    PrepareConfHandler prepareConfHandler;

    long confToken;

    @BeforeEach
    void setUp() {

        bytes = new ByteArrayOutputStream();
        console = System.out;
        System.setOut(new PrintStream(bytes));

        filters = new HashMap<>();
        filters.put("UpstreamFilter", new PluginFilter() {
                    @Override
                    public String name() {
                        return "UpstreamFilter";
                    }

                    @Override
                    public void postFilter(PostRequest request, PostResponse response, PluginFilterChain chain) {
                        System.out.println("do post filter: UpStreamFilter, order: " + chain.getIndex());
                        System.out.println("do post filter: UpStreamFilter, conf: " + request.getConfig(this));
                        System.out.println("do post filter: UpStreamFilter, upstreamStatusCode: " + request.getUpstreamStatusCode());
                        for (Map.Entry<String, String> header : request.getUpstreamHeaders().entrySet()) {
                            System.out.println("do post filter: UpStreamFilter, upstreamHeader key: " + header.getKey());
                            System.out.println("do post filter: UpStreamFilter, upstreamHeader value: " + header.getValue());
                        }

                        chain.postFilter(request, response);
                    }
                }
        );
        watchers = new ArrayList<>();
        cache = CacheBuilder.newBuilder().expireAfterWrite(3600, TimeUnit.SECONDS).maximumSize(1000).build();
        FlatBufferBuilder builder = new FlatBufferBuilder();

        int foo = builder.createString("UpstreamFilter");
        int bar = builder.createString("{\"conf_key1\":\"conf_value1\",\"conf_key2\":2}");
        int filter = TextEntry.createTextEntry(builder, foo, bar);

        int confVector = io.github.api7.A6.PrepareConf.Req.createConfVector(builder, new int[]{filter});
        io.github.api7.A6.PrepareConf.Req.startReq(builder);
        io.github.api7.A6.PrepareConf.Req.addConf(builder, confVector);
        builder.finish(io.github.api7.A6.PrepareConf.Req.endReq(builder));
        io.github.api7.A6.PrepareConf.Req req = io.github.api7.A6.PrepareConf.Req.getRootAsReq(builder.dataBuffer());

        A6ConfigRequest request = new A6ConfigRequest(req);
        prepareConfHandler = new PrepareConfHandler(cache, filters, watchers);
        channel = new EmbeddedChannel(new BinaryProtocolDecoder(), prepareConfHandler);
        channel.writeInbound(request);
        channel.finish();
        A6ConfigResponse response = channel.readOutbound();
        confToken = response.getConfToken();

        prepareConfHandler = new PrepareConfHandler(cache, filters, watchers);
        rpcCallHandler = new RpcCallHandler(cache);
        channel = new EmbeddedChannel(new BinaryProtocolDecoder(), prepareConfHandler, rpcCallHandler);
    }

    @AfterEach
    void setDown() {
        System.setOut(console);
    }

    @Test
    @DisplayName("test doPostFilter")
    void doPostFilter() {
        FlatBufferBuilder builder = new FlatBufferBuilder();

        int headerKey = builder.createString("headerKey");
        int headerValue = builder.createString("headerValue");

        int header = TextEntry.createTextEntry(builder, headerKey, headerValue);
        int headerVector =
                io.github.api7.A6.HTTPRespCall.Req.createHeadersVector(builder, new int[]{header});
        io.github.api7.A6.HTTPRespCall.Req.startReq(builder);
        io.github.api7.A6.HTTPRespCall.Req.addId(builder, 8888L);
        io.github.api7.A6.HTTPRespCall.Req.addConfToken(builder, confToken);
        io.github.api7.A6.HTTPRespCall.Req.addStatus(builder, 418);
        io.github.api7.A6.HTTPRespCall.Req.addHeaders(builder, headerVector);
        builder.finish(io.github.api7.A6.HTTPRespCall.Req.endReq(builder));
        io.github.api7.A6.HTTPRespCall.Req req = io.github.api7.A6.HTTPRespCall.Req.getRootAsReq(builder.dataBuffer());
        PostRequest request = new PostRequest(req);
        channel.writeInbound(request);
        channel.finish();
        PostResponse response = channel.readOutbound();

        Assertions.assertTrue(bytes.toString().contains("do post filter: UpStreamFilter, order: 1"));
        Assertions.assertTrue(bytes.toString().contains("do post filter: UpStreamFilter, conf: {\"conf_key1\":\"conf_value1\",\"conf_key2\":2}"));
        Assertions.assertTrue(bytes.toString().contains("do post filter: UpStreamFilter, upstreamStatusCode: 418"));
        Assertions.assertTrue(bytes.toString().contains("do post filter: UpStreamFilter, upstreamHeader key: headerKey"));
        Assertions.assertTrue(bytes.toString().contains("do post filter: UpStreamFilter, upstreamHeader value: headerValue"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("test upstream fields survive buffer release before extra info returns")
    void doPostFilterAfterBufferRelease(boolean responseBody) {
        PluginFilter filter = new PluginFilter() {
            @Override
            public String name() {
                return "UpstreamFilter";
            }

            @Override
            public List<String> requiredVars() {
                return responseBody ? null : Collections.singletonList("server_port");
            }

            @Override
            public Boolean requiredRespBody() {
                return responseBody;
            }

            @Override
            public void postFilter(PostRequest request, PostResponse response, PluginFilterChain chain) {
                Assertions.assertEquals(Collections.singletonMap("x-upstream", "original"), request.getUpstreamHeaders());
                Assertions.assertEquals(418, request.getUpstreamStatusCode());
                Assertions.assertEquals(8888L, request.getRequestId());
                if (responseBody) {
                    Assertions.assertEquals("upstream-body", request.getBody());
                } else {
                    Assertions.assertEquals("9080", request.getVars("server_port"));
                }
                chain.postFilter(request, response);
            }
        };
        cache.put(confToken, new A6Conf(Collections.emptyMap(),
                new PluginFilterChain(Collections.singletonList(filter))));

        FlatBufferBuilder builder = new FlatBufferBuilder();
        int header = TextEntry.createTextEntry(builder,
                builder.createString("x-upstream"), builder.createString("original"));
        int headers = io.github.api7.A6.HTTPRespCall.Req.createHeadersVector(builder, new int[]{header});
        io.github.api7.A6.HTTPRespCall.Req.startReq(builder);
        io.github.api7.A6.HTTPRespCall.Req.addId(builder, 8888L);
        io.github.api7.A6.HTTPRespCall.Req.addConfToken(builder, confToken);
        io.github.api7.A6.HTTPRespCall.Req.addStatus(builder, 418);
        io.github.api7.A6.HTTPRespCall.Req.addHeaders(builder, headers);
        builder.finish(io.github.api7.A6.HTTPRespCall.Req.endReq(builder));
        byte[] frame = frame((byte) 4, builder.dataBuffer());
        ByteBuf input = Unpooled.wrappedBuffer(frame);

        EmbeddedChannel postChannel = new EmbeddedChannel(new BinaryProtocolDecoder(),
                new PayloadDecoder(), new RpcCallHandler(cache));
        try {
            postChannel.writeInbound(input);
            Assertions.assertEquals(0, input.refCnt());
            Assertions.assertTrue(postChannel.readOutbound() instanceof ExtraInfoRequest);
            Assertions.assertNull(postChannel.readOutbound());

            // Deterministically simulate reuse of the released frame's backing memory.
            Arrays.fill(frame, (byte) 0);

            builder = new FlatBufferBuilder();
            int result = io.github.api7.A6.ExtraInfo.Resp.createResultVector(builder,
                    (responseBody ? "upstream-body" : "9080").getBytes(StandardCharsets.UTF_8));
            builder.finish(io.github.api7.A6.ExtraInfo.Resp.createResp(builder, result));
            postChannel.writeInbound(Unpooled.wrappedBuffer(frame((byte) 3, builder.dataBuffer())));

            Assertions.assertTrue(postChannel.readOutbound() instanceof PostResponse);
            Assertions.assertNull(postChannel.readOutbound());
        } finally {
            postChannel.finishAndReleaseAll();
        }
    }

    private byte[] frame(byte type, ByteBuffer payload) {
        int length = payload.remaining();
        return ByteBuffer.allocate(length + 4).put(type)
                .put((byte) (length >> 16)).put((byte) (length >> 8)).put((byte) length)
                .put(payload).array();
    }
}
