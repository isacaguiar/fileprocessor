package com.example.fileprocessor;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DirectoryProcessorTest {

  @TempDir
  Path tempDir;

  private static void write(Path file, String content) throws IOException {
    Files.createDirectories(file.getParent());
    Files.write(file, Collections.singleton(content),
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
  }

  private static String read(Path file) throws IOException {
    return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("Processes a flat file and writes UPPERCASE to the output directorya")
  void processSingleFile_toUppercase_andCreatesOutput() throws Exception {
    Path inDir = tempDir.resolve("in");
    Path outDir = tempDir.resolve("out");
    Files.createDirectories(inDir);

    Path input = inDir.resolve("a.txt");
    write(input, "Hello\nworld\nCamelCase");

    DirectoryProcessor processor = new DirectoryProcessor(inDir, outDir, 4);
    processor.execute();

    Path output = outDir.resolve("a.txt");
    assertTrue(Files.exists(output), "Output file not created");

    String expected = String.join(System.lineSeparator(),
        "HELLO","WORLD","CAMELCASE") + System.lineSeparator();
    assertEquals(expected, read(output), "Content converted to UPPERCASE does not match");
  }

  @Test
  @DisplayName("Maintains folder structure and processes multiple files .txt")
  void processNestedFiles_andMirrorStructure() throws Exception {
    Path inDir = tempDir.resolve("in");
    Path outDir = tempDir.resolve("out");
    Files.createDirectories(inDir);

    Path f1 = inDir.resolve("root.txt");
    Path f2 = inDir.resolve("nested/inner.txt");
    Path ignored = inDir.resolve("nested/ignore.md");

    write(f1, "abc");
    write(f2, "xYz");
    write(ignored, "should not be processed");

    DirectoryProcessor processor = new DirectoryProcessor(inDir, outDir, 2);
    processor.execute();

    // .txt processed
    assertTrue(Files.exists(outDir.resolve("root.txt")));
    assertTrue(Files.exists(outDir.resolve("nested/inner.txt")));
    // does not create output for non-txt
    assertFalse(Files.exists(outDir.resolve("nested/ignore.md")));

    // valida conteúdo upper
    assertEquals("ABC" + System.lineSeparator(), read(outDir.resolve("root.txt")));
    assertEquals("XYZ" + System.lineSeparator(), read(outDir.resolve("nested/inner.txt")));
  }

  @Test
  @DisplayName("Empty input directory does not generate files and terminates without erroro")
  void emptyInputDir_finishesGracefully() throws Exception {
    Path inDir = tempDir.resolve("in");
    Path outDir = tempDir.resolve("out");
    Files.createDirectories(inDir);

    DirectoryProcessor processor = new DirectoryProcessor(inDir, outDir, 1);
    processor.execute();

    assertTrue(Files.exists(outDir), "Output directory should exist");
    // There are no files to process
    try (Stream<Path> stream = Files.walk(outDir)) {
      long files = stream.filter(Files::isRegularFile).count();
      assertEquals(0L, files, "No file should have been generated");
    }
  }
}
