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
package org.apache.rocketmq.spring.support;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.rocketmq.spring.support.RocketMQUtil.toRocketHeaderKey;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RocketMQUtilTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testMessageBuilder() {
        Message<String> msg = MessageBuilder.withPayload("test").
                setHeader("A", "test1").
                setHeader("B", "test2").
                build();
        System.out.printf("header size=%d  %s %n", msg.getHeaders().size(), msg.getHeaders());
        assertNotNull(msg.getHeaders().get("A"));
        assertNotNull(msg.getHeaders().get("B"));
    }

    @Test
    void testPayload() {
        String charset = "UTF-8";
        String destination = "test-topic";
        Message<String> msgWithStringPayload = MessageBuilder.withPayload("test").build();
        org.apache.rocketmq.common.message.Message rocketMsg1 = RocketMQUtil.convertToRocketMessage(objectMapper,
                charset, destination, msgWithStringPayload);

        Message<byte[]> msgWithBytePayload = MessageBuilder.withPayload("test".getBytes()).build();
        org.apache.rocketmq.common.message.Message rocketMsg2 = RocketMQUtil.convertToRocketMessage(objectMapper,
                charset, destination, msgWithBytePayload);

        assertArrayEquals((msgWithStringPayload.getPayload()).getBytes(), rocketMsg1.getBody());
        assertArrayEquals(msgWithBytePayload.getPayload(), rocketMsg2.getBody());
    }

    @Test
    void testHeaderConvertToRMQMsg() {
        Message<String> msgWithStringPayload = MessageBuilder.withPayload("test body")
                .setHeader("test", 1)
                .setHeader(RocketMQHeaders.TAGS, "tags")
                .setHeader(RocketMQHeaders.KEYS, "my_keys")
                .build();
        org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                "UTF-8", "test-topic", msgWithStringPayload);
        assertEquals("1", rocketMsg.getProperty("test"));
        assertNull(rocketMsg.getProperty(RocketMQHeaders.TAGS));
        assertEquals("my_keys", rocketMsg.getProperty(RocketMQHeaders.KEYS));
    }

    @Test
    void testHeaderConvertToSpringMsg() {
        org.apache.rocketmq.common.message.Message rmqMsg = new org.apache.rocketmq.common.message.Message();
        rmqMsg.setBody("test body".getBytes());
        rmqMsg.setTopic("test-topic");
        rmqMsg.putUserProperty("test", "1");
        rmqMsg.setTags("tags");
        Message springMsg = RocketMQUtil.convertToSpringMessage(rmqMsg);
        assertEquals("1", springMsg.getHeaders().get("test"));
        assertEquals("tags", springMsg.getHeaders().get(RocketMQHeaders.PREFIX + RocketMQHeaders.TAGS));

        org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                "UTF-8", "test-topic", springMsg);
        assertEquals("1", rocketMsg.getProperty("test"));
        assertEquals("tags", rocketMsg.getProperty(RocketMQHeaders.PREFIX + RocketMQHeaders.TAGS));
        assertNull(rocketMsg.getTags());

        rmqMsg.putUserProperty(toRocketHeaderKey(RocketMQHeaders.TAGS), "tags2");
        springMsg = RocketMQUtil.convertToSpringMessage(rmqMsg);
        assertEquals("tags", springMsg.getHeaders().get(RocketMQHeaders.PREFIX + RocketMQHeaders.TAGS));
    }

    @Test
    void testConvertToRocketMessageWithMessageConvert() {
        Message<String> msgWithStringPayload = MessageBuilder.withPayload("test body")
                .setHeader("test", 1)
                .setHeader(RocketMQHeaders.TAGS, "tags")
                .setHeader(RocketMQHeaders.KEYS, "my_keys")
                .build();
        RocketMQMessageConverter messageConverter = new RocketMQMessageConverter();
        org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(messageConverter.getMessageConverter(),
                "UTF-8", "test-topic", msgWithStringPayload);
        assertEquals("1", rocketMsg.getProperty("test"));
        assertNull(rocketMsg.getProperty(RocketMQHeaders.TAGS));
        assertEquals("my_keys", rocketMsg.getProperty(RocketMQHeaders.KEYS));

        Message<byte[]> msgWithBytesPayload = MessageBuilder.withPayload("123".getBytes()).build();
        org.apache.rocketmq.common.message.Message rocketMsgWithObj = RocketMQUtil.convertToRocketMessage(messageConverter.getMessageConverter(),
                "UTF-8", "test-topic", msgWithBytesPayload);
        assertEquals("123", new String(rocketMsgWithObj.getBody()));
    }

    @Test
    void testConvertLocalDateTimeWithRocketMQMessageConverter() {
        TestMessage message = new TestMessage("localDateTime test",
                LocalDateTime.of(2022, 3, 7, 12, 0, 0));
        String str = new String(JSON.toJSONString(message).getBytes(), StandardCharsets.UTF_8);
        RocketMQMessageConverter messageConverter = new RocketMQMessageConverter();
        Object obj = messageConverter.getMessageConverter().fromMessage(MessageBuilder.withPayload(str).build(), TestMessage.class);
        assertTrue(obj instanceof TestMessage);
        assertEquals("localDateTime test", ((TestMessage) obj).getBody());
        assertEquals("2022-03-07 12:00:00",
                ((TestMessage) obj).getTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }

    @Test
    void testConvertToSpringMessage() {
        org.apache.rocketmq.common.message.MessageExt rocketMsg = new org.apache.rocketmq.common.message.MessageExt();
        rocketMsg.setTopic("test");
        rocketMsg.setBody("test".getBytes());
        rocketMsg.setTags("tagA");
        rocketMsg.setKeys("key1");
        Message message = RocketMQUtil.convertToSpringMessage(rocketMsg);
        assertEquals("test", message.getHeaders().get(toRocketHeaderKey(RocketMQHeaders.TOPIC)));
        assertEquals("tagA", message.getHeaders().get(toRocketHeaderKey(RocketMQHeaders.TAGS)));
        assertEquals("key1", message.getHeaders().get(toRocketHeaderKey(RocketMQHeaders.KEYS)));
    }

    private String removeNanoTime(String instanceName) {
        int index = instanceName.lastIndexOf('@');
        if (index < 0) {
            return instanceName;
        }
        return instanceName.substring(0, index);
    }

    static class TestMessage {
        private String body;
        private LocalDateTime time;

        public TestMessage() {

        }

        public TestMessage(String body, LocalDateTime time) {
            this.body = body;
            this.time = time;
        }

        public LocalDateTime getTime() {
            return time;
        }

        public String getBody() {
            return body;
        }
    }
}
