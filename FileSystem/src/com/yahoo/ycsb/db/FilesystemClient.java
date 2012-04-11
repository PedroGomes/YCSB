package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.io.*;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

/**
 * Class for the measure of the system raw performance
 */
public class FilesystemClient extends DB {

    public static final int Ok = 0;
    public static final int Error = -1;

    public static final String BASE_FOLDER_PROPERTY = "filesystem.base_folder";

    /**The base folder for the base system*/
    private String filesystem_base_folder;


    public void init() throws DBException {
        filesystem_base_folder = getProperties().getProperty(BASE_FOLDER_PROPERTY);
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

        File file = new File(filesystem_base_folder+"/"+key);
        FileReader fis = null;
        BufferedReader bufferedReader = null;

        try {
            fis = new FileReader(file);
            bufferedReader = new BufferedReader(fis) ;

            String line;
            String line2 = null;

            while ((line = bufferedReader.readLine()) != null) {
                line2 = line;
            }
            if (line2!=null) {
                int l = line2.length();
            }

            fis.close();
            bufferedReader.close();

            return Ok;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Error;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return Error;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return Error;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return Error;
    }

    @Override
    public int delete(String table, String key) {
        return Error;
    }
}
