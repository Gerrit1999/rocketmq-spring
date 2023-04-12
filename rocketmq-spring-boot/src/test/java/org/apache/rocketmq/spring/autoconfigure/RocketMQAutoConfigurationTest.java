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

package org.apache.rocketmq.spring.autoconfigure;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.spring.annotation.ExtRocketMQConsumerConfiguration;
import org.apache.rocketmq.spring.annotation.ExtRocketMQTemplateConfiguration;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.core.RocketMQReplyListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQMessageConverter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RocketMQAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RocketMQAutoConfiguration.class));

    @Test
    void testDefaultMQProducerNotCreatedByDefault() {
        // You will see the WARN log message about missing rocketmq.name-server spring property when running this test case.
        assertThrows(NoSuchBeanDefinitionException.class,
                () -> runner.run(context -> context.getBean(DefaultMQProducer.class)));
    }

    @Test
    void testDefaultLitePullConsumerNotCreatedByDefault() {
        // You will see the WARN log message about missing rocketmq.name-server spring property when running this test case.
        assertThrows(NoSuchBeanDefinitionException.class,
                () -> runner.run(context -> context.getBean(DefaultLitePullConsumer.class)));
    }

    @Test
    void testDefaultMQProducerWithRelaxPropertyName() {
        runner.withPropertyValues("rocketmq.nameServer=127.0.0.1:9876",
                        "rocketmq.producer.group=spring_rocketmq",
                        "rocketmq.accessChannel=LOCAL").
                run((context) -> {
                    assertThat(context).hasSingleBean(DefaultMQProducer.class);
                    assertThat(context).hasSingleBean(RocketMQProperties.class);
                });

    }

    @Test
    void testDefaultLitePullConsumerWithRelaxPropertyName() {
        runner.withPropertyValues("rocketmq.nameServer=127.0.0.1:9876",
                        "rocketmq.pull-consumer.group=spring_rocketmq",
                        "rocketmq.pull-consumer.topic=test",
                        "rocketmq.accessChannel=LOCAL").
                run((context) -> {
                    assertThat(context).hasSingleBean(DefaultLitePullConsumer.class);
                    assertThat(context).hasSingleBean(RocketMQProperties.class);
                });

    }

    @Test
    void testBadAccessChannelProperty() {
        runner.withPropertyValues("rocketmq.nameServer=127.0.0.1:9876",
                        "rocketmq.producer.group=spring_rocketmq",
                        "rocketmq.accessChannel=LOCAL123").
                run((context) -> {
                    //Should throw exception for bad accessChannel property
                    assertThat(context).getFailure();
                });

        runner.withPropertyValues("rocketmq.nameServer=127.0.0.1:9876",
                        "rocketmq.pull-consumer.group=spring_rocketmq",
                        "rocketmq.pull-consumer.topic=test",
                        "rocketmq.accessChannel=LOCAL123").
                run((context) -> {
                    //Should throw exception for bad accessChannel property
                    assertThat(context).getFailure();
                });
    }

    @Test
    void testDefaultMQProducer() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876",
                        "rocketmq.producer.group=spring_rocketmq").
                run((context) -> assertThat(context).hasSingleBean(DefaultMQProducer.class));
    }

    @Test
    void testDefaultLitePullConsumer() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876",
                        "rocketmq.pull-consumer.group=spring_rocketmq",
                        "rocketmq.pull-consumer.topic=test").
                run((context) -> assertThat(context).hasSingleBean(DefaultLitePullConsumer.class));
    }

    @Test
    void testExtRocketMQTemplate() {
        runner.withPropertyValues("rocketmq.name-server=127.0.1.1:9876").
                withUserConfiguration(TestExtRocketMQTemplateConfig.class, CustomObjectMappersConfig.class).
                run((context) -> {
                    // No producer on consume side
                    assertThat(context).getBean("extRocketMQTemplate").hasFieldOrProperty("producer");
                    // Auto-create consume container if existing Bean annotated with @RocketMQMessageListener
                });
    }


    @Test
    void testExtRocketMQConsumer() {
        runner.withPropertyValues("rocketmq.name-server=127.0.1.1:9876").
                withUserConfiguration(TestExtRocketMQConsumerConfig.class, CustomObjectMappersConfig.class).
                run((context) -> assertThat(context).getBean("extRocketMQTemplate").hasFieldOrProperty("consumer"));
    }

    @Test
    void testConsumerListener() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876",
                        "rocketmq.producer.group=spring_rocketmq",
                        "rocketmq.consumer.listeners.spring_rocketmq.FOO_TEST_TOPIC=false",
                        "rocketmq.consumer.listeners.spring_rocketmq.FOO_TEST_TOPIC2=true").
                run((context) -> {
                    RocketMQProperties rocketMQProperties = context.getBean(RocketMQProperties.class);
                    assertThat(rocketMQProperties.getConsumer().getListeners().get("spring_rocketmq").get("FOO_TEST_TOPIC").booleanValue()).isEqualTo(false);
                    assertThat(rocketMQProperties.getConsumer().getListeners().get("spring_rocketmq").get("FOO_TEST_TOPIC2").booleanValue()).isEqualTo(true);
                });

    }

    @Test
    void testRocketMQTransactionListener() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876",
                        "rocketmq.producer.group=spring_rocketmq",
                        "demo.rocketmq.transaction.producer.group=transaction-group1").
                withUserConfiguration(TestTransactionListenerConfig.class).
                run((context) -> {
                    assertThat(context).hasSingleBean(TestRocketMQLocalTransactionListener.class);
                    RocketMQTransactionListener annotation = TestRocketMQLocalTransactionListener.class.getAnnotation(RocketMQTransactionListener.class);
                    RocketMQTemplate rocketMQTemplate = (RocketMQTemplate) context.getBean(annotation.rocketMQTemplateBeanName());
                    ThreadPoolExecutor executor = (ThreadPoolExecutor) ((TransactionMQProducer) rocketMQTemplate.getProducer()).getExecutorService();
                    assertThat(executor.getKeepAliveTime(TimeUnit.SECONDS)).isEqualTo(50);
                });
    }

    @Test
    void testBatchSendMessage() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876",
                        "rocketmq.producer.group=spring_rocketmq").
                run((context) -> {
                    RocketMQTemplate rocketMQTemplate = context.getBean(RocketMQTemplate.class);
                    List<GenericMessage<String>> batchMessages = new ArrayList<>();

                    String errorMsg = null;
                    try {
                        SendResult customSendResult = rocketMQTemplate.syncSend("test", batchMessages, 60000);
                    } catch (IllegalArgumentException e) {
                        // it will be throw IllegalArgumentException: `messages` can not be empty
                        errorMsg = e.getMessage();
                    }

                    // that means the rocketMQTemplate.syncSend is chosen the correct type method
                    assertEquals("`messages` can not be empty", errorMsg);
                });

    }

    void testPlaceholdersListenerContainer() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876",
                        "demo.placeholders.consumer.group = abc3",
                        "demo.placeholders.consumer.topic = test",
                        "demo.placeholders.consumer.tags = tag1").
                withUserConfiguration(TestPlaceholdersConfig.class).
                run((context) -> {
                    // No producer on consume side
                    assertThat(context).doesNotHaveBean(DefaultMQProducer.class);
                    // Auto-create consume container if existing Bean annotated with @RocketMQMessageListener
                    assertThat(context).hasBean("org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer_1");
                    assertThat(context).getBean("org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer_1").
                            hasFieldOrPropertyWithValue("nameServer", "127.0.0.1:9876").
                            hasFieldOrPropertyWithValue("consumerGroup", "abc3").
                            hasFieldOrPropertyWithValue("topic", "test").
                            hasFieldOrPropertyWithValue("selectorExpression", "tag1");
                });
    }

    @Test
    void testRocketMQListenerContainer() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876").
                withUserConfiguration(TestConfig.class).
                run((context) -> assertThat(context).getFailure().hasMessageContaining("connect to null failed"));
    }

    @Test
    void testRocketMQListenerContainer_RocketMQReplyListener() {
        runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876").
                withUserConfiguration(TestConfigWithRocketMQReplyListener.class).
                run((context) -> assertThat(context).getFailure().hasMessageContaining("connect to null failed"));
    }

    @Test
    void testRocketMQListenerContainer_WrongRocketMQListenerType() {
        assertThrows(IllegalStateException.class,
                () -> runner.withPropertyValues("rocketmq.name-server=127.0.0.1:9876").
                        withUserConfiguration(TestConfigWithWrongRocketMQListener.class).
                        run((context) -> context.getBean(RocketMQMessageConverter.class)));
    }

    @Configuration
    static class TestConfig {

        @Bean
        public Object consumeListener() {
            return new TestDefaultNameServerListener();
        }

        @Bean
        public Object consumeListener1() {
            return new TestCustomNameServerListener();
        }

    }

    @Configuration
    static class TestConfigWithRocketMQReplyListener {

        @Bean
        public Object consumeListener() {
            return new TestDefaultNameServerRocketMQReplyListener();
        }

        @Bean
        public Object consumeListener1() {
            return new TestCustomNameServerRocketMQReplyListener();
        }

    }

    @Configuration
    static class TestConfigWithWrongRocketMQListener {

        @Bean
        public Object consumeListener() {
            return new WrongRocketMQListener();
        }
    }

    @Configuration
    static class CustomObjectMapperConfig {

        @Bean
        public RocketMQMessageConverter rocketMQMessageConverter() {
            return new RocketMQMessageConverter();
        }

    }

    @Configuration
    static class CustomObjectMappersConfig {

        @Bean
        public RocketMQMessageConverter rocketMQMessageConverter() {
            return new RocketMQMessageConverter();
        }

    }

    @RocketMQMessageListener(consumerGroup = "abc", topic = "test")
    static class TestDefaultNameServerListener implements RocketMQListener {

        @Override
        public void onMessage(Object message) {

        }
    }

    @RocketMQMessageListener(nameServer = "127.0.1.1:9876", consumerGroup = "abc1", topic = "test")
    static class TestCustomNameServerListener implements RocketMQListener {

        @Override
        public void onMessage(Object message) {

        }
    }

    @RocketMQMessageListener(consumerGroup = "abcd", topic = "test")
    static class TestDefaultNameServerRocketMQReplyListener implements RocketMQReplyListener<String, String> {

        @Override
        public String onMessage(String message) {
            return "test";
        }
    }

    @RocketMQMessageListener(consumerGroup = "abcde", topic = "test")
    static class WrongRocketMQListener {

        public String onMessage(String message) {
            return "test";
        }
    }

    @RocketMQMessageListener(nameServer = "127.0.1.1:9876", consumerGroup = "abcd1", topic = "test")
    static class TestCustomNameServerRocketMQReplyListener implements RocketMQReplyListener<String, String> {

        @Override
        public String onMessage(String message) {
            return "test";
        }
    }

    @Configuration
    static class TestTransactionListenerConfig {
        @Bean
        public Object rocketMQLocalTransactionListener() {
            return new TestRocketMQLocalTransactionListener();
        }

    }

    @RocketMQTransactionListener(keepAliveTime = 50, keepAliveTimeUnit = TimeUnit.SECONDS)
    static class TestRocketMQLocalTransactionListener implements RocketMQLocalTransactionListener {

        @Override
        public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            return RocketMQLocalTransactionState.COMMIT;
        }

        @Override
        public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
            return RocketMQLocalTransactionState.COMMIT;
        }
    }

    @Configuration
    static class TestExtRocketMQTemplateConfig {

        @Bean
        public RocketMQTemplate extRocketMQTemplate() {
            return new TestExtRocketMQTemplate();
        }

    }

    @ExtRocketMQTemplateConfiguration(group = "test", nameServer = "127.0.0.1:9876")
    static class TestExtRocketMQTemplate extends RocketMQTemplate {

    }

    @Configuration
    static class TestPlaceholdersConfig {

        @Bean
        public Object consumeListener() {
            return new TestPlaceholdersListener();
        }

    }

    @RocketMQMessageListener(consumerGroup = "${demo.placeholders.consumer.group}", topic = "${demo.placeholders.consumer.topic}", selectorExpression = "${demo.placeholders.consumer.tags}")
    static class TestPlaceholdersListener implements RocketMQListener {

        @Override
        public void onMessage(Object message) {

        }
    }

    @Configuration
    static class TestExtRocketMQConsumerConfig {

        @Bean
        public RocketMQTemplate extRocketMQTemplate() {
            return new TestExtRocketMQConsumer();
        }

    }

    @ExtRocketMQConsumerConfiguration(topic = "test", group = "test", nameServer = "127.0.0.1:9876")
    static class TestExtRocketMQConsumer extends RocketMQTemplate {

    }
}

