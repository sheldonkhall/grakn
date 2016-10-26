/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.loader;

import io.mindmaps.engine.postprocessing.BackgroundTasks;
import io.mindmaps.engine.postprocessing.Cache;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.util.ErrorMessage;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.mindmaps.engine.loader.TransactionState.State;
import static io.mindmaps.graql.Graql.withGraph;

/**
 * Singleton class that handles insert queries received via REST end point.
 * It also maintains the statistics about the loading jobs.
 */

public class RESTLoader {
    private final Logger LOG = LoggerFactory.getLogger(RESTLoader.class);

    private Cache cache;

    private static int repeatCommits;

    private static RESTLoader instance = null;

    private ExecutorService executor;
    private final Map<UUID, TransactionState> loaderState;

    private AtomicInteger enqueuedJobs;
    private AtomicInteger loadingJobs;
    private AtomicInteger finishedJobs;
    private AtomicLong lastJobFinished;
    private AtomicInteger errorJobs;

    public long getLastJobFinished() {
        return lastJobFinished.get();
    }

    public int getLoadingJobs() {
        return loadingJobs.get();
    }

    private RESTLoader() {
        ConfigProperties prop = ConfigProperties.getInstance();

        cache = Cache.getInstance();
        loaderState = new ConcurrentHashMap<>();
        enqueuedJobs = new AtomicInteger();
        loadingJobs = new AtomicInteger();
        errorJobs = new AtomicInteger();
        finishedJobs = new AtomicInteger();
        lastJobFinished = new AtomicLong();
        repeatCommits = prop.getPropertyAsInt(ConfigProperties.LOADER_REPEAT_COMMITS);

        int numberThreads = prop.getAvailableThreads();
        executor = Executors.newFixedThreadPool(numberThreads);

        startPeriodPostProcessingCheck();
    }


    public static synchronized RESTLoader getInstance() {
        if (instance == null) instance = new RESTLoader();
        return instance;
    }

    private void startPeriodPostProcessingCheck() {
        long postProcessingDelay = ConfigProperties.getInstance().getPropertyAsLong(ConfigProperties.POSTPROCESSING_DELAY);
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> BackgroundTasks.getInstance().performPostprocessing(), postProcessingDelay,
                        postProcessingDelay, TimeUnit.MILLISECONDS);
    }

    public String getLoaderState() {
        return Json.object().set(State.QUEUED.name(), enqueuedJobs.get())
                .set(State.LOADING.name(), loadingJobs.get())
                .set(State.ERROR.name(), errorJobs.get())
                .set(State.FINISHED.name(), finishedJobs.get()).toString();
    }


    public UUID addJob(String name, String queryString) {
        UUID newUUID = UUID.randomUUID();
        loaderState.put(newUUID, new TransactionState(State.QUEUED));
        executor.submit(() -> loadData(name, queryString, newUUID));
        enqueuedJobs.incrementAndGet();
        return newUUID;
    }

    public void loadData(String name, String batch, UUID uuid) {
        // Attempt committing the transaction a certain number of times
        // If a transaction fails, it must be repeated from scratch because Titan is forgetful
        loaderState.put(uuid, new TransactionState(State.LOADING));
        enqueuedJobs.decrementAndGet();

        loadingJobs.incrementAndGet();

        try {
            AbstractMindmapsGraph graph = (AbstractMindmapsGraph) GraphFactory.getInstance().getGraphBatchLoading(name);
            for (int i = 0; i < repeatCommits; i++) {

                try {
                    withGraph(graph).parse(batch).execute();
                    graph.commit();
                    cache.addJobCasting(name, graph.getModifiedCastingIds());
                    cache.addJobResource(name, graph.getModifiedResourceIds());
                    loaderState.get(uuid).setState(State.FINISHED);
                    finishedJobs.incrementAndGet();
                    return;

                } catch (MindmapsValidationException e) {
                    //If it's a validation exception there is no point in re-trying
                    LOG.error("Input batch: {}", batch);
                    LOG.error("Caused Exception: {}", ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
                    loaderState.get(uuid).setState(State.ERROR);
                    loaderState.get(uuid).setException(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
                    errorJobs.incrementAndGet();
                    return;
                } catch (IllegalArgumentException e) {
                    //If it's an illegal argument exception there is no point in re-trying
                    LOG.error("Input batch: {}", batch);
                    LOG.error("Caused Exception: {}", ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION.getMessage(e.getMessage()));
                    loaderState.get(uuid).setState(State.ERROR);
                    loaderState.get(uuid).setException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION.getMessage(e.getMessage()));
                    errorJobs.incrementAndGet();
                    return;
                } catch (Exception e) {
                    //If it's not a validation exception we need to remain in the for loop
                    handleError(e, 1);
                } finally {
                    try {
                        lastJobFinished.set(System.currentTimeMillis());
                    } catch (Exception e) {
                        LOG.error("Exception", e);
                    }
                }
            }
        } finally {
            loadingJobs.decrementAndGet();
        }

        LOG.error("Input batch: {}", batch);
        LOG.error("Caused Exception: {}", ErrorMessage.FAILED_TRANSACTION.getMessage(repeatCommits));

        loaderState.get(uuid).setState(State.ERROR);
        loaderState.get(uuid).setException(ErrorMessage.FAILED_TRANSACTION.getMessage(repeatCommits));
        errorJobs.incrementAndGet();
    }

    public String getStatus(UUID uuid) {
        return loaderState.get(uuid).toString();
    }

    private void handleError(Exception e, int i) {
        LOG.error("Caught exception while trying to commit a new transaction.", e);
        try {
            Thread.sleep((i + 2) * 1000);
        } catch (InterruptedException e1) {
            LOG.error("Caught exception ", e1);
        }
    }

}
