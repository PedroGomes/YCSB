package com.yahoo.ycsb.db;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.index.keys.KeysIndex;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.commons.cli.*;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
* Class for data extraction from SStables
 */
public class SStableExtractor {

    private static final String OUTFILE_OPTION = "o";
    private static final String INFILE_OPTION = "i";
    private static final String KEY_OPTION = "k";
    private static Options options;
    private static CommandLine cmd;

    static
    {
        options = new Options();
        Option optOutfile = new Option(OUTFILE_OPTION, true, "output file");
        optOutfile.setRequired(false);
        options.addOption(optOutfile);

    }

    public static  String[] gatherFile(String folder) throws Exception{

        System.out.println("Selected folder:" + folder);
        File table_folder = new File(folder);
        if(!table_folder.exists()){
            throw new Exception("Folder does not exist");
        }
        if(!table_folder.isDirectory())
        {
            throw new Exception("The path does not correspond to a folder");
        }
        FilenameFilter filenameFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.trim().endsWith("-Data.db"); 
            }
        };

        String[] sstables = table_folder.list(filenameFilter);

        if(sstables.length==0){
            throw new Exception("No SStables in the selected folder");
        }

        return sstables;
    }   


    public static void export(String folder, String outfile) throws Exception {
        String[] sstables = gatherFile(folder);
        int size = 0;
        for( String sstable : sstables ){
            Descriptor descriptor = Descriptor.fromFilename(folder+"/"+sstable);
            CFMetaData metadata;
            if (descriptor.cfname.contains("."))
            {
                // look up index metadata from parent
                int i = descriptor.cfname.indexOf(".");
                String parentName = descriptor.cfname.substring(0, i);
                CFMetaData parent = Schema.instance.getCFMetaData(descriptor.ksname, parentName);
                ColumnDefinition def = parent.getColumnDefinitionForIndex(descriptor.cfname.substring(i + 1));
                metadata = CFMetaData.newIndexMetadata(parent, def, KeysIndex.indexComparator());
            }
            else
            {
                metadata = Schema.instance.getCFMetaData(descriptor.ksname, descriptor.cfname);
            }
            size += export(SSTableReader.open(descriptor, metadata));
        }
        System.out.println("Total read: "+size);
    }

    // This is necessary to accommodate the test suite since you cannot open a Reader more
    // than once from within the same process.
    static int export(SSTableReader reader) throws IOException
    {
        Set<String> excludeSet = new HashSet<String>();

        SSTableIdentityIterator row;
        SSTableScanner scanner = reader.getDirectScanner();

//        outs.println("{");

        int i = 0;

        // collecting keys to export
        while (scanner.hasNext())
        {
            row = (SSTableIdentityIterator) scanner.next();

            String currentKey = bytesToHex(row.getKey().key);

            //System.out.println(currentKey);
            if (excludeSet.contains(currentKey))
                continue;
            else if (i != 0)
                //outs.println(",");

          //  serializeRow(row, row.getKey(), outs);

            i++;
        }

        scanner.close();
        return i;
    }



    public static void main(String[] args) throws IOException
    {
        String usage = String.format("Usage: %s -o <outfile> -i <sstable_folder> %n",
                SStableExtractor.class.getName());

        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        } catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }

        String outFile = cmd.getOptionValue(OUTFILE_OPTION);


        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable folder");
            System.err.println(usage);
            System.exit(1);
        }

        DatabaseDescriptor.loadSchemas();
        if (Schema.instance.getNonSystemTables().size() < 1)
        {
            String msg = "no non-system tables are defined";
            System.err.println(msg);
        }



        try {
            export(cmd.getArgs()[0], outFile);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.exit(0);
    }


}
