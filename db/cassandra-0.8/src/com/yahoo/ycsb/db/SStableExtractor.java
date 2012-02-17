package com.yahoo.ycsb.db;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.index.keys.KeysIndex;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.cli.*;
//import redis.clients.jedis.Jedis;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.string;


import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Class for data extraction from SStables
 */
public class SStableExtractor {

    private static final int NUMBER_THREADS = 1;
    private static final String OUTFILE_OPTION = "o";
    private static final String INFILE_OPTION = "i";
    private static final String KEY_OPTION = "k";
    private static Options options;
    private static CommandLine cmd;

    static {
        options = new Options();
        Option optOutfile = new Option(OUTFILE_OPTION, true, "output file");
        optOutfile.setRequired(false);
        options.addOption(optOutfile);

    }

    public static String[] gatherFile(String folder) throws Exception {

        System.out.println("Selected folder:" + folder);
        File table_folder = new File(folder);
        if (!table_folder.exists()) {
            throw new Exception("Folder does not exist");
        }
        if (!table_folder.isDirectory()) {
            throw new Exception("The path does not correspond to a folder");
        }
        FilenameFilter filenameFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.trim().endsWith("-Data.db");
            }
        };

        String[] sstables = table_folder.list(filenameFilter);

        if (sstables.length == 0) {
            throw new Exception("No SStables in the selected folder");
        }

        return sstables;
    }


    public static int export(String folder, String outfile) throws Exception {
        String[] sstables = gatherFile(folder);
        System.out.println(Arrays.toString(sstables));
        int size = 0;

        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_THREADS);
        List<Future<Long>> reader_threads_results = new ArrayList<Future<Long>>();

        for (String sstable : sstables) {
            Descriptor descriptor = Descriptor.fromFilename(folder + "/" + sstable);
            CFMetaData metadata;
            if (descriptor.cfname.contains(".")) {
                // look up index metadata from parent
                int i = descriptor.cfname.indexOf(".");
                String parentName = descriptor.cfname.substring(0, i);
                CFMetaData parent = Schema.instance.getCFMetaData(descriptor.ksname, parentName);
                ColumnDefinition def = parent.getColumnDefinitionForIndex(descriptor.cfname.substring(i + 1));
                metadata = CFMetaData.newIndexMetadata(parent, def, KeysIndex.indexComparator());
            } else {
                metadata = Schema.instance.getCFMetaData(descriptor.ksname, descriptor.cfname);
            }

            ExportingThread exportingThread = new ExportingThread(SSTableReader.open(descriptor, metadata));

            Future<Long> submit = executor.submit(exportingThread);
            reader_threads_results.add(submit);

        }

        for (Future<Long> future : reader_threads_results) {
            try {
                size += future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
        return size;
    }


    // This is necessary to accommodate the test suite since you cannot open a Reader more
    // than once from within the same process.
    static int export(SSTableReader reader) throws IOException {
        Set<String> excludeSet = new HashSet<String>();

        SSTableIdentityIterator row;
        SSTableScanner scanner = reader.getDirectScanner();

//        outs.println("{");

        int i = 0;

        // collecting keys to export
        while (scanner.hasNext()) {
            row = (SSTableIdentityIterator) scanner.next();

            String currentKey = bytesToHex(row.getKey().key);

            //System.out.println(currentKey);
            if (excludeSet.contains(currentKey))
                continue;
            else if (i != 0) {
                //outs.println(",");
            }

            //  serializeRow(row, row.getKey(), outs);

            i++;
        }

        scanner.close();
        return i;
    }


    public static void main(String[] args) throws IOException {
        String usage = String.format("Usage: %s -o <outfile> <sstable_folder> %n",
                SStableExtractor.class.getName());

        CommandLineParser parser = new PosixParser();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e1) {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }

        String outFile = cmd.getOptionValue(OUTFILE_OPTION);


        if (cmd.getArgs().length != 1) {
            System.err.println("You must supply exactly one sstable folder");
            System.err.println(usage);
            System.exit(1);
        }

        DatabaseDescriptor.loadSchemas();
        if (Schema.instance.getNonSystemTables().size() < 1) {
            String msg = "no non-system tables are defined";
            System.err.println(msg);
        }

        try {

            long ti = System.currentTimeMillis();
            int number_read_lines = export(cmd.getArgs()[0], outFile);
            long tf = System.currentTimeMillis();

            System.out.println("Number of read lines: " + number_read_lines + " in " + ((tf - ti) / 1000) + "s (" + ((tf - ti) / 60000) + "m)");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.exit(0);
    }
}

class ExportingThread implements Callable<Long> {

    SSTableReader reader;
    //Jedis redis_client;
    TreeMap<String, SSTableIdentityIterator> retrieved_rows;

    ExportingThread(SSTableReader reader) {
        this.reader = reader;
    }

    @Override
    public Long call() {

        retrieved_rows = new TreeMap<String, SSTableIdentityIterator>();
        //    redis_client = new Jedis("192.168.111.220", 6379);

        long read_lines = 0;


        SSTableScanner scanner = reader.getDirectScanner();

//        outs.println("{");

        // collecting keys to export
        while (scanner.hasNext()) {
            SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();

            String key = null;
            try {
                key = string(row.getKey().key);
            } catch (CharacterCodingException e) {
             //   key = row.getPath();
            }
            if (key != null) {
//                if (!retrieved_rows.containsKey(key)) {
//                    retrieved_rows.put(key, row);
//                } else {
//                    System.out.println(key +" : "+serialize_row(row) + "-|-" + serialize_row(retrieved_rows.get(key)));
//                }
                read_lines++;
            }
        }

        try {
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return read_lines;
    }

    private static String serialize_row(SSTableIdentityIterator row) {

        ColumnFamily columnFamily = row.getColumnFamily();
        CFMetaData cfMetaData = columnFamily.metadata();
        AbstractType comparator = columnFamily.getComparator();
        StringBuffer stringBuffer = new StringBuffer("{");

        while (row.hasNext()) {
            IColumn column = row.next();
            List<Object> columns = serializeColumn(column, comparator, cfMetaData);
            stringBuffer.append(columns.toString());
            stringBuffer.append("||");
        }
        stringBuffer.append("}");

        return stringBuffer.toString();
    }


    private static List<Object> serializeColumn(IColumn column, AbstractType comparator, CFMetaData cfMetaData) {

        ArrayList<Object> serializedColumn = new ArrayList<Object>();

        ByteBuffer name = ByteBufferUtil.clone(column.name());
        ByteBuffer value = ByteBufferUtil.clone(column.value());

        serializedColumn.add(comparator.getString(name));
//        if (column instanceof DeletedColumn) {
//            serializedColumn.add(ByteBufferUtil.bytesToHex(value));
//        } else {
//            AbstractType validator = cfMetaData.getValueValidator(name);
//            serializedColumn.add(validator.getString(value));
//        }
        serializedColumn.add(column.timestamp());

        if (column instanceof DeletedColumn) {
            serializedColumn.add("d");
        } else if (column instanceof ExpiringColumn) {
            serializedColumn.add("e");
            serializedColumn.add(((ExpiringColumn) column).getTimeToLive());
            serializedColumn.add(column.getLocalDeletionTime());
        }
        return serializedColumn;
    }

}