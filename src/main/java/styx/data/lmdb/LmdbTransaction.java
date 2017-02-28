package styx.data.lmdb;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import styx.data.db.DatabaseTransaction;
import styx.data.db.Path;
import styx.data.db.Row;

class LmdbTransaction implements DatabaseTransaction {

    private final Transaction txn;
    private final Database dbi;

    LmdbTransaction(Transaction txn, Database dbi) {
        this.txn = txn;
        this.dbi = dbi;
    }

    @Override
    public void close() {
        if(!txn.isReadOnly()) {
            txn.commit();
        }
        txn.close();
    }

    @Override
    public Stream<Row> selectAll() {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            List<Row> rows = new ArrayList<>();
            if(cursor.first()) {
                do {
                    rows.add(readRow(cursor));
                } while(cursor.next());
            }
            return rows.stream();
        }
    }

    @Override
    public Optional<Row> selectSingle(Path parent, String key) {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            writeKey(cursor, parent, key);
            if(cursor.seekKey()) {
                return Optional.of(readRow(cursor));
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public Stream<Row> selectChildren(Path parent) {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            byte[] keyPrefix = writeKeyPrefix(cursor, parent, true);
            List<Row> rows = new ArrayList<>();
            if(cursor.seekRange()) {
                do {
                    if(!startsWith(cursor, keyPrefix)) {
                        break;
                    }
                    rows.add(readRow(cursor));
                } while(cursor.next());
            }
            return rows.stream();
        }
    }

    @Override
    public Stream<Row> selectDescendants(Path parent) {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            byte[] keyPrefix = writeKeyPrefix(cursor, parent, false);
            List<Row> rows = new ArrayList<>();
            if(cursor.seekRange()) {
                do {
                    if(!startsWith(cursor, keyPrefix)) {
                        break;
                    }
                    rows.add(readRow(cursor));
                } while(cursor.next());
            }
            return rows.stream().sorted(Row.ITERATION_ORDER);
        }
    }

    @Override
    public int allocateSuffix(Path parent) {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            byte[] keyPrefix = writeKeyPrefix(cursor, parent, true);
            int maxSuffix = 0;
            if(cursor.seekRange()) {
                do {
                    if(!startsWith(cursor, keyPrefix)) {
                        break;
                    }
                    maxSuffix = Math.max(maxSuffix, readSuffix(cursor));
                } while(cursor.next());
            }
            return maxSuffix + 1;
        }
    }

    @Override
    public void insert(Row row) {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            writeKey(cursor, row.parent(), row.key());
            if(cursor.seekKey()) {
                throw new IllegalStateException();
            }
            writeValue(cursor, row);
            cursor.put();
        }
    }

    @Override
    public void deleteAll() {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            if(cursor.first()) {
                do {
                    cursor.delete();
                } while(cursor.next());
            }
        }
    }

    @Override
    public void deleteSingle(Path parent, String key) {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            writeKey(cursor, parent, key);
            if(cursor.seekKey()) {
                cursor.delete();
            }
        }
    }

    @Override
    public void deleteDescendants(Path parent) {
        try(BufferCursor cursor = dbi.bufferCursor(txn)) {
            byte[] keyPrefix = writeKeyPrefix(cursor, parent, false);
            if(cursor.seekRange()) {
                do {
                    if(!startsWith(cursor, keyPrefix)) {
                        break;
                    }
                    cursor.delete();
                } while(cursor.next());
            }
        }
    }

    private static boolean startsWith(BufferCursor cursor, byte[] keyPrefix) {
        int keyLength = cursor.keyLength();
        if(keyLength < keyPrefix.length) {
            return false;
        }
        for(int i = 0; i < keyPrefix.length; i++) {
            if(cursor.keyByte(i) != keyPrefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static Row readRow(BufferCursor cursor) {
        int keySepPos = getKeySepPos(cursor);
        int valSepPos = getValSepPos(cursor);
        Path parent = Path.decode(new String(cursor.keyBytes(0, keySepPos), StandardCharsets.UTF_8));
        String key = new String(cursor.keyBytes(keySepPos+1, cursor.keyLength()-keySepPos-1), StandardCharsets.UTF_8);
        int suffix = 0;
        String value = null;
        if(valSepPos > 0) {
            suffix = Integer.parseInt(new String(cursor.valBytes(0, valSepPos), StandardCharsets.UTF_8));
        }
        if(valSepPos+1 < cursor.valLength()) {
            value = new String(cursor.valBytes(valSepPos+1, cursor.valLength()-valSepPos-1), StandardCharsets.UTF_8);
        }
        return new Row(parent, key, suffix, value);
    }

    private static int readSuffix(BufferCursor cursor) {
        int valSepPos = getValSepPos(cursor);
        if(valSepPos > 0) {
            return Integer.parseInt(new String(cursor.valBytes(0, valSepPos), StandardCharsets.UTF_8));
        } else {
            return 0;
        }
    }

    private static void writeKey(BufferCursor cursor, Path parent, String key) {
        cursor.keyWriteBytes(parent.encode().getBytes(StandardCharsets.UTF_8));
        cursor.keyWriteByte('\t');
        cursor.keyWriteBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] writeKeyPrefix(BufferCursor cursor, Path parent, boolean sep) {
        String string = parent.encode();
        if(sep) {
            string += "\t";
        }
        byte[] keyPrefix = string.getBytes(StandardCharsets.UTF_8);
        cursor.keyWriteBytes(keyPrefix);
        return keyPrefix;
    }

    private static void writeValue(BufferCursor cursor, Row row) {
        if(row.suffix() != 0) {
            cursor.valWriteBytes(Integer.toString(row.suffix()).getBytes(StandardCharsets.UTF_8));
        }
        cursor.valWriteByte('\t');
        if(row.value() != null) {
            cursor.valWriteBytes(row.value().getBytes(StandardCharsets.UTF_8));
        }
    }

    private static int getKeySepPos(BufferCursor cursor) {
        int length = cursor.keyLength();
        int sepPos = 0;
        while(sepPos < length && cursor.keyByte(sepPos) != '\t') {
            sepPos++;
        }
        if(sepPos == length) {
            throw new IllegalStateException();
        }
        return sepPos;
    }

    private static int getValSepPos(BufferCursor cursor) {
        int length = cursor.valLength();
        int sepPos = 0;
        while(sepPos < length && cursor.valByte(sepPos) != '\t') {
            sepPos++;
        }
        if(sepPos == length) {
            throw new IllegalStateException();
        }
        return sepPos;
    }
}
