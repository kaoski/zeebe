/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import static io.zeebe.test.UpgradeTestCaseProvider.PROCESS_ID;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.util.VersionUtil;
import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Execution(ExecutionMode.SAME_THREAD)
class UpgradeTest {
  private static final String LAST_VERSION = VersionUtil.getPreviousVersion();
  private static final String CURRENT_VERSION = "current-test";

  private ContainerState state = new ContainerState();

  @RegisterExtension ContainerStateExtension stateExtension = new ContainerStateExtension(state);

  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(UpgradeTestCaseProvider.class)
  void oldGatewayWithNewBroker(final String name, final UpgradeTestCase testCase) {
    // given
    state.broker(CURRENT_VERSION).withStandaloneGateway(LAST_VERSION).start(true);
    final long wfInstanceKey = testCase.setUp(state.client());

    // when
    final long key = testCase.runBefore(state);

    // then
    testCase.runAfter(state, wfInstanceKey, key);
    Awaitility.await("until process is completed")
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> assertThat(state.hasElementInState(PROCESS_ID, "ELEMENT_COMPLETED")).isTrue());
  }

  @ExtendWith(ContainerStateExtension.class)
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(UpgradeTestCaseProvider.class)
  void upgradeWithSnapshot(final String name, final UpgradeTestCase testCase) {
    upgradeZeebe(testCase, true);
  }

  @ExtendWith(ContainerStateExtension.class)
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(UpgradeTestCaseProvider.class)
  void upgradeWithoutSnapshot(final String name, final UpgradeTestCase testCase) {
    upgradeZeebe(testCase, false);
  }

  private void upgradeZeebe(final UpgradeTestCase testCase, final boolean withSnapshot) {
    // given
    state.broker(LAST_VERSION).start(true);
    final long wfInstanceKey = testCase.setUp(state.client());
    final long key = testCase.runBefore(state);

    // when
    final File snapshot = new File(state.getVolumePath(), "raft-partition/partitions/1/snapshots/");

    if (withSnapshot) {
      state.close();
      state.broker(LAST_VERSION).start(false);

      // since 0.24, no snapshot is created when the broker is closed
      Awaitility.await()
          .atMost(Duration.ofMinutes(2))
          .untilAsserted(
              () ->
                  assertThat(snapshot)
                      .describedAs("Expected that a snapshot is created")
                      .exists()
                      .isNotEmptyDirectory());

      state.close();

    } else {
      // since 0.24, no snapshot is created when the broker is closed
      state.close();

      assertThat(snapshot)
          .describedAs("Expected that no snapshot is created")
          .exists()
          .isEmptyDirectory();
    }

    // then
    state.broker(CURRENT_VERSION).start(true);
    testCase.runAfter(state, wfInstanceKey, key);

    Awaitility.await("until process is completed")
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> assertThat(state.hasElementInState(PROCESS_ID, "ELEMENT_COMPLETED")).isTrue());
  }
}
