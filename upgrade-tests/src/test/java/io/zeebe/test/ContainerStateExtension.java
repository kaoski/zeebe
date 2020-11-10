/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.agrona.IoUtil;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerStateExtension
    implements BeforeTestExecutionCallback, AfterTestExecutionCallback {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerStateExtension.class);
  private static final File SHARED_DATA;

  static {
    final var sharedDataPath =
        Optional.ofNullable(System.getenv("ZEEBE_CI_SHARED_DATA"))
            .map(Paths::get)
            .orElse(Paths.get(System.getProperty("tmpdir", "/tmp"), "shared"));
    SHARED_DATA = sharedDataPath.toAbsolutePath().toFile();
    IoUtil.ensureDirectoryExists(SHARED_DATA, "temporary folder for Docker");
  }

  private final ContainerState state;
  private Path sharedData;

  public ContainerStateExtension() {
    this(new ContainerState());
  }

  public ContainerStateExtension(final ContainerState state) {
    this.state = state;
  }

  public ContainerState getState() {
    return state;
  }

  public Path getSharedData() {
    return sharedData;
  }

  @Override
  public void afterTestExecution(final ExtensionContext context) throws Exception {
    try {
      state.close();
    } catch (final Exception e) {
      LOG.warn("Failed to close container state", e);
    }

    if (sharedData != null) {
      try {
        FileUtil.deleteFolder(sharedData);
      } catch (final Exception e) {
        LOG.warn("Failed to delete shared data directory", e);
      }
    }
  }

  @Override
  public void beforeTestExecution(final ExtensionContext context) throws Exception {
    try {
      sharedData = Files.createTempDirectory(SHARED_DATA.toPath(), "sharedData");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    state.withVolumePath(sharedData);
  }
}
