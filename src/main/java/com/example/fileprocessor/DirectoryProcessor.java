package com.example.fileprocessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryProcessor {

  private static final Logger log = LoggerFactory.getLogger(DirectoryProcessor.class);

  private final Path inputDir;
  private final Path outputDir;
  private final int nThreads;


  private final LongAdder processedLines = new LongAdder();
  DirectoryProcessor(Path inputDir, Path outputDir, int nThreads) {
    this.inputDir = inputDir;
    this.outputDir = outputDir;
    this.nThreads = nThreads;
  }

  public static void main(String[] args) throws InterruptedException {
    Path inputDir  = Paths.get(args.length > 0 ? args[0] : "in");
    Path outputDir = Paths.get(args.length > 1 ? args[1] : "out");
    int nThreads   = args.length > 2 ? Integer.parseInt(args[2]) : 6;

    DirectoryProcessor processor =
        new DirectoryProcessor(inputDir, outputDir, nThreads);
    log.info("Started with params inputDir: {} outputDir {}, nThreads {}.", inputDir, outputDir, nThreads);
    processor.execute();
    log.info("Finish...");
  }

  protected void execute() throws InterruptedException {
    createDirectory();
    ExecutorService pool = initPool();
    List<Path> files = collectorFiles();
    List<Future<Path>> futures = process(pool, files);
    StateExecution stateExecution = waiting(futures);
    finishing(pool,stateExecution);
  }

  private void createDirectory() {
    try {
      if (Files.exists(outputDir)) {
        if (!Files.isDirectory(outputDir)) {
          log.error("File with name exists: {}", outputDir);
          throw new RuntimeException("File with name exists: " + outputDir);
        }
        log.debug("Directory exists: {}", outputDir);
      } else {
        Files.createDirectories(outputDir);
        log.debug("Directory created: {}", outputDir);
      }
    } catch (IOException e) {
      log.error("Could not create/verify output folder: {}", outputDir, e);
      throw new RuntimeException("Could not create/verify output folder: " + outputDir, e);
    }
  }

  /**
   * Collects text files
   */
  private List<Path> collectorFiles() {
    List<Path> files;
    try {
      try (Stream<Path> stream = Files.walk(inputDir)) {
        files = stream
            .filter(Files::isRegularFile)
            .filter(p -> p.toString().endsWith(".txt"))
            .collect(Collectors.toList());
      }
    } catch (IOException e) {
      log.error("Error listing files in {}", inputDir, e);
      throw new RuntimeException("Error listing files in " + inputDir, e);
    }
    return files;
  }

  private ExecutorService initPool() {
    return Executors.newFixedThreadPool(nThreads);
  }

  private List<Future<Path>> process(ExecutorService pool, List<Path> files) {
    log.info("Files to process: {}", files.size());

    List<Future<Path>> futures = new ArrayList<>();

    int taskId = 0;
    for (Path in : files) {
      taskId++;
      log.info("Executing task {} in thread {}", taskId, Thread.currentThread().getName());
      int finalTaskId = taskId;
      futures.add(pool.submit(() -> {
        Path rel   = inputDir.relativize(in);
        Path out   = outputDir.resolve(rel);
        Files.createDirectories(out.getParent());

        try (BufferedReader br = Files.newBufferedReader(in, StandardCharsets.UTF_8);
             BufferedWriter bw = Files.newBufferedWriter(out, StandardCharsets.UTF_8,
                 StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

          String line;
          while ((line = br.readLine()) != null) {
            String up = line.toUpperCase(Locale.ROOT);
            bw.write(up);
            bw.newLine();
            processedLines.increment();
          }
        } catch (IOException e) {
          log.error("Failed to process: {}", in, e);
          throw new IOException("Failed to process: " + in, e);
        }
        log.info("Task completed {}", finalTaskId);
        return out;
      }));
    }

    return futures;
  }

  /**
   * Waits for and reports correct errors
   */
  private StateExecution waiting(List<Future<Path>> futures) {
    StateExecution stateExecution = new StateExecution();
    for (Future<Path> f : futures) {
      try {
        f.get();
        stateExecution.incrementOk();
      } catch (ExecutionException ee) {
        stateExecution.incrementFail();
        log.error("Error: {}", ee.getCause().getMessage(), ee);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.error("Interrupted waiting for tasks", ie);
        throw new RuntimeException("Interrupted waiting for tasks", ie);
      }
    }
    return stateExecution;
  }

  private void finishing(ExecutorService pool, StateExecution stateExecution) throws InterruptedException {
    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.MINUTES);

    log.info("Created: {} | Fail: {}", stateExecution.getOk(), stateExecution.getFail());
    log.info("Lines processed: {}", processedLines.sum());
  }

  static class StateExecution {
    private int ok;
    private int fail;

    StateExecution() {
      this.ok = 0;
      this.fail = 0;
    }

    public void incrementOk() {
      ok++;
    }

    public void incrementFail() {
      fail++;
    }

    public int getOk() {
      return ok;
    }

    public int getFail() {
      return fail;
    }
  }
}
