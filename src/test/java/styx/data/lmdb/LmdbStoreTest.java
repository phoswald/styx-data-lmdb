package styx.data.lmdb;

import java.nio.file.Path;
import java.nio.file.Paths;

import styx.data.GenericStoreTest;

public class LmdbStoreTest extends GenericStoreTest {

    private static final Path file = Paths.get("target/test/LmdbDatabaseTest/store.lmdb");

    public LmdbStoreTest() {
        super("lmdb:" + file);
    }
}
