/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import io.atomix.raft.partition.RaftPartition;
import io.zeebe.broker.Loggers;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.slf4j.Logger;

public class PartitionProcessingState {

  public static final Logger LOG = Loggers.SYSTEM_LOGGER;
  private static final String PERSISTED_PAUSE_STATE_FILENAME = ".paused";
  private boolean isProcessingPaused;
  private final RaftPartition raftPartition;
  private boolean diskSpaceAvailable;

  public PartitionProcessingState(final RaftPartition raftPartition) {
    this.raftPartition = raftPartition;
    initProcessingStatus();
  }

  public boolean isDiskSpaceAvailable() {
    return diskSpaceAvailable;
  }

  public void setDiskSpaceAvailable(final boolean diskSpaceAvailable) {
    this.diskSpaceAvailable = diskSpaceAvailable;
  }

  public boolean isProcessingPaused() {
    return isProcessingPaused;
  }

  @SuppressWarnings({"squid:S899"})
  public void setProcessingPaused(final boolean processingPaused) throws IOException {
    final File persistedPauseState = getPersistedPauseState();
    if (processingPaused) {
      persistedPauseState.createNewFile();
      if (persistedPauseState.exists()) {
        isProcessingPaused = processingPaused;
      }
    } else {
      Files.deleteIfExists(persistedPauseState.toPath());
      if (!persistedPauseState.exists()) {
        isProcessingPaused = processingPaused;
      }
    }
  }

  private File getPersistedPauseState() {
    return raftPartition.dataDirectory().toPath().resolve(PERSISTED_PAUSE_STATE_FILENAME).toFile();
  }

  private void initProcessingStatus() {
    final boolean pauseProcessing = getPersistedPauseState().exists();
    isProcessingPaused = pauseProcessing;
  }

  public boolean shouldProcess() {
    return isDiskSpaceAvailable() && !isProcessingPaused();
  }
}
