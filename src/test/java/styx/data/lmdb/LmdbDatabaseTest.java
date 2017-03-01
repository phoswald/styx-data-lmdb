package styx.data.lmdb;

import java.nio.file.Path;
import java.nio.file.Paths;

import styx.data.db.GenericDatabaseTest;

public class LmdbDatabaseTest extends GenericDatabaseTest {

    private static final Path file = Paths.get("target/test/datastore.lmdb");

    public LmdbDatabaseTest() {
        super(LmdbDatabase.open(file.toString()));
    }
}
