/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.measurements.ResultHandler;
import com.yahoo.ycsb.measurements.ResultStorage;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.transport.TTransportException;


//XXXX if we do replication, fix the consistency levels

/**
 * Cassandra 0.8 client for YCSB framework
 */
public class CassandraClient8 extends DB {

    private static AtomicInteger clientID_source = new AtomicInteger(0);
    private int clientID;

    static Random random = new Random();
    public static final int Ok = 0;
    public static final int Error = -1;
    public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

    public int ConnectionRetries;
    public int OperationRetries;
    public String column_family;

    public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
    public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

    public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
    public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

    public static final String USERNAME_PROPERTY = "cassandra.username";
    public static final String PASSWORD_PROPERTY = "cassandra.password";

    public static final String READ_CONSISTENCY_PROPERTY = "cassandra.readconsistency";
    public static final String SCAN_CONSISTENCY_PROPERTY = "cassandra.scanconsistency";
    public static final String WRITE_CONSISTENCY_PROPERTY = "cassandra.writeconsistency";

    public static final String ROW_BUFFER_PROPERTY = "cassandra.rowbuffer";
    public static final String COLUMN_BUFFER_PROPERTY = "cassandra.columnbuffer";
    public static final String COLUMN_BUFFER_DEFAULT_PROPERTY = "200";


    public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
    public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

    public static final String SCAN_CONNECTIONS_PROPERTY = "cassandra.scan_connections";

    public static final String TOKENIZED_SCANS_PROPERTY = "cassandra.tokenized_scans";

    public static final String DEBUG_FOLDER_PROPERTY = "cassandra.debug_folder";

    String username;
    String password;

    String debug_folder;

    String[] available_client_hosts;
    
    TTransport tr;
    Cassandra.Client client;
    String selected_host;

    boolean using_scan_connection = false;

    Map<String, TTransport> scan_cons_trp;
    Map<String, Cassandra.Client> scan_clients;

    public boolean tokenized_scans = false;


    boolean _debug = false;

    String _table = "";
    String _scan_table = "";

    Exception errorexception = null;

    List<Mutation> mutations = new ArrayList<Mutation>();
    Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
    Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

    ColumnParent parent;

    int row_buffer = 1000;
    int column_buffer;
    ConsistencyLevel read_ConsistencyLevel = ConsistencyLevel.QUORUM;
    ConsistencyLevel write_ConsistencyLevel = ConsistencyLevel.QUORUM;
    ConsistencyLevel scan_ConsistencyLevel = ConsistencyLevel.QUORUM;
    
    ResultHandler error_logger;
    int timout_tries = 0;


    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void init() throws DBException {
        clientID = clientID_source.incrementAndGet();
        error_logger = ResultStorage.getClientResultHandlerInstance(clientID+"");

        String hosts = getProperties().getProperty("hosts");
        if (hosts == null) {
            throw new DBException("Required property \"hosts\" missing for CassandraClient");
        }

        column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
        parent = new ColumnParent(column_family);

        ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
                CONNECTION_RETRY_PROPERTY_DEFAULT));
        OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
                OPERATION_RETRY_PROPERTY_DEFAULT));

        username = getProperties().getProperty(USERNAME_PROPERTY);
        password = getProperties().getProperty(PASSWORD_PROPERTY);

        row_buffer = Integer.parseInt(getProperties().getProperty(ROW_BUFFER_PROPERTY, "1000"));
        column_buffer = Integer.parseInt(getProperties().getProperty(COLUMN_BUFFER_PROPERTY,COLUMN_BUFFER_DEFAULT_PROPERTY));

        try {

            read_ConsistencyLevel = getConsistencyLevel(getProperties().getProperty(READ_CONSISTENCY_PROPERTY, "QUORUM"));
            write_ConsistencyLevel = getConsistencyLevel(getProperties().getProperty(WRITE_CONSISTENCY_PROPERTY, "QUORUM"));
            scan_ConsistencyLevel = getConsistencyLevel(getProperties().getProperty(SCAN_CONSISTENCY_PROPERTY, "ONE"));
        } catch (Exception e) {
            throw new DBException(e);
        }

        _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        available_client_hosts = hosts.split(",");
        String myhost = available_client_hosts[random.nextInt(available_client_hosts.length)];
        selected_host = myhost;
        Pair<Cassandra.Client, TTransport> client_connection = makeConnection(username, password, myhost);
        client = client_connection.getLeft();
        tr = client_connection.getRight();

        scan_cons_trp = new TreeMap<String, TTransport>();
        scan_clients = new TreeMap<String, Cassandra.Client>();

        String[] available_scan_hosts;
        String scan_hosts = getProperties().getProperty(SCAN_CONNECTIONS_PROPERTY);
        if (scan_hosts != null && !scan_hosts.trim().isEmpty()) {
            using_scan_connection = true;
            available_scan_hosts = scan_hosts.split(",");
            for (String cass_scan_host : available_scan_hosts) {
                client_connection = makeConnection(username, password, cass_scan_host);
                scan_clients.put(cass_scan_host, client_connection.getLeft());
                scan_cons_trp.put(cass_scan_host, client_connection.getRight());
            }

        }

        tokenized_scans = Boolean.parseBoolean(getProperties().getProperty(TOKENIZED_SCANS_PROPERTY, "false"));

        debug_folder = getProperties().getProperty(DEBUG_FOLDER_PROPERTY);

    }

    public Pair<Cassandra.Client, TTransport> makeConnection(String username, String password, String myhost) throws DBException {

        TTransport connection_tr = null;
        Cassandra.Client db_client = null;

        Exception connectexception = null;

        for (int retry = 0; retry < ConnectionRetries; retry++) {
            connection_tr = new TFramedTransport(new TSocket(myhost, 9160));
            TProtocol proto = new TBinaryProtocol(connection_tr);
            db_client = new Cassandra.Client(proto);
            try {
                connection_tr.open();
                connectexception = null;
                break;
            } catch (Exception e) {
                connectexception = e;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        if (connectexception != null) {
            System.err.println("Unable to connect to " + myhost + " after " + ConnectionRetries
                    + " tries");
            System.out.println("Unable to connect to " + myhost + " after " + ConnectionRetries
                    + " tries");
            throw new DBException(connectexception);
        }

        if (username != null && password != null) {
            Map<String, String> cred = new HashMap<String, String>();
            cred.put("username", username);
            cred.put("password", password);
            AuthenticationRequest req = new AuthenticationRequest(cred);
            try {
                db_client.login(req);
            } catch (Exception e) {
                throw new DBException(e);
            }
        }
        return new Pair<Cassandra.Client, TTransport>(db_client, connection_tr);
    }


    public ConsistencyLevel getConsistencyLevel(String consistency_string) throws Exception {
        ConsistencyLevel consistencyLevel = null;
        if (consistency_string.equalsIgnoreCase("ONE")) {
            consistencyLevel = ConsistencyLevel.ONE;
        } else if (consistency_string.equalsIgnoreCase("TWO")) {
            consistencyLevel = ConsistencyLevel.TWO;
        } else if (consistency_string.equalsIgnoreCase("THREE")) {
            consistencyLevel = ConsistencyLevel.THREE;
        } else if (consistency_string.equalsIgnoreCase("QUORUM")) {
            consistencyLevel = ConsistencyLevel.QUORUM;
        } else if (consistency_string.equalsIgnoreCase("EACH_QUORUM")) {
            consistencyLevel = ConsistencyLevel.EACH_QUORUM;
        } else if (consistency_string.equalsIgnoreCase("LOCAL_QUORUM")) {
            consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        } else if (consistency_string.equalsIgnoreCase("ALL")) {
            consistencyLevel = ConsistencyLevel.ALL;
        } else if (consistency_string.equalsIgnoreCase("ANY")) {
            consistencyLevel = ConsistencyLevel.ANY;
        } else {
            throw new Exception("Consistency level no recognised");
        }
        return consistencyLevel;
    }


    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    public void cleanup() throws DBException {
        if (using_scan_connection) {
            for (TTransport scan_con_trp : scan_cons_trp.values())
                scan_con_trp.close();
        }
    }

    /**
     * Log the error occurred in the client
     */
    public void logError(String method,Exception e, String host) {
        ArrayList<Object> error_elements = new ArrayList<Object>();
        error_elements.add(clientID);
        error_elements.add(host);
        error_elements.add(e.getClass().toString());
        error_elements.add(method);
        error_elements.add(System.currentTimeMillis());

        error_logger.logData("Errors",error_elements);
    }

    /**
     *
     * @param method  the calling method
     * @param e  the exception
     * @param host the used client host when the error occurred
     * @return a flag that tells to the calling method if it should quit
     */
    public synchronized boolean handleError(String method,Exception e, String host) {

        logError(method,e,host);

        if (e instanceof TimedOutException) {
            timout_tries++;
            if (timout_tries != OperationRetries) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                }
            }
        } else if (e instanceof InvalidRequestException || e instanceof UnavailableException) {
            e.printStackTrace();
            //System.out.println("Nothing we can do");
            timout_tries = OperationRetries;
        } else if (e instanceof TApplicationException || e instanceof TTransportException) {
            
            boolean connected = false;

            ArrayList<String> available_connections = new ArrayList<String>(Arrays.asList(available_client_hosts));
            available_connections.remove(host);
            while(!connected){

                if(available_connections.isEmpty()){
                    return true;
                }

                String myhost = selected_host;
                while(myhost.equals(selected_host)){
                    myhost = available_connections.get(random.nextInt(available_connections.size()));
                }

                selected_host = myhost;
                Pair<Cassandra.Client, TTransport> client_connection = null;
                try {
                    client_connection = makeConnection(username, password, myhost);
                } catch (DBException e1) {
                    available_connections.remove(host);
                    continue;
                }
                client = client_connection.getLeft();
                tr = client_connection.getRight();

                try {
                    client.set_keyspace(_table);
                } catch (Exception e1) {
                    e.printStackTrace();
                    available_connections.remove(host);
                    continue;
                }

                connected = true;
            }

        } else {
            System.out.println("Unknown exception: " + e.getCause());
        }
        

        return OperationRetries==timout_tries;

    }


    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        if (!_table.equals(table)) {
            try {
                client.set_keyspace(table);
                _table = table;
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return Error;
            }
        }
        timout_tries =0;
        boolean quit = false;
        while (!quit){
        //for (int i = 0; i < OperationRetries; i++) {

            try {
                SlicePredicate predicate;
                if (fields == null) {
                    predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));

                } else {
                    ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
                    for (String s : fields) {
                        fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
                    }

                    predicate = new SlicePredicate().setColumn_names(fieldlist);
                }

                List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, read_ConsistencyLevel);

                if (_debug) {
                    System.out.print("Reading key: " + key);
                }

                Column column;
                String name;
                ByteIterator value;
                for (ColumnOrSuperColumn oneresult : results) {

                    column = oneresult.column;
                    name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                    value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());

                    result.put(name, value);

                    if (_debug) {
                        System.out.print("(" + name + "=" + value + ")");
                    }
                }

                if (_debug) {
                    System.out.println();
                }

                return Ok;
            } catch (Exception e) {
                errorexception = e;
                if(handleError("Read",e,selected_host)){
                    quit = true;
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
     //   System.out.println("error: "+errorexception.getClass().toString()+" : "+errorexception.getMessage());
        return Error;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a HashMap.
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value
     *                    pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
                    Vector<HashMap<String, ByteIterator>> result) {

        Cassandra.Client used_client = null;
        String scan_host = "";
        if (using_scan_connection) {
            int selected = random.nextInt(scan_clients.size());
            int i = 0;
            for (String host : scan_clients.keySet()) {
                if (i == selected)
                    scan_host = host;
                    used_client = scan_clients.get(host);
                i++;
            }
        } else {
            scan_host=selected_host;
            used_client = client;
        }

        for (int i = 0; i < OperationRetries; i++) {

            try {
                SlicePredicate predicate;
                if (fields == null) {
//                    predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));
                    predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, column_buffer));

                } else {
                    ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
                    for (String s : fields) {
                        fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
                    }

                    predicate = new SlicePredicate().setColumn_names(fieldlist);
                }

                int limit = (recordcount < row_buffer) ? recordcount : row_buffer;

                List<KeySlice> results = new ArrayList<KeySlice>();
                int size = 0;

                if (tokenized_scans) {
                    List<TokenRange> tr = used_client.describe_ring("Eurotux");

                    Map<String, List<Pair<String, String>>> token_endpoints = new HashMap<String, List<Pair<String, String>>>();
                    //  Map<String,Pair<String,String>> token_endpoints = new HashMap<String, Pair<String, String>>();

                    for (TokenRange trange : tr) {
                        String start_token = trange.getStart_token();
                        String end_token = trange.getEnd_token();
                        for (EndpointDetails epd : trange.getEndpoint_details()) {
                            if (epd.getDatacenter().equals("DC2")) {
                                if (token_endpoints.containsKey(epd.getHost())) {
                                    token_endpoints.get(epd.getHost()).add(new Pair<String, String>(start_token, end_token));
                                } else {
                                    List<Pair<String, String>> endpoints_slices = new ArrayList<Pair<String, String>>();
                                    endpoints_slices.add(new Pair<String, String>(start_token, end_token));
                                    token_endpoints.put(epd.getHost(), endpoints_slices);
                                }
                            }
                        }
                    }

                    List<Pair<Thread, Token_scan>> scan_threads = new LinkedList<Pair<Thread, Token_scan>>();

                    for (String endpoint_host : token_endpoints.keySet()) {

                        used_client = scan_clients.get(endpoint_host);
                        if (!_scan_table.equals(table)) {
                            used_client.set_keyspace(table);
                        }

                        Token_scan token_scan = new Token_scan(token_endpoints.get(endpoint_host), endpoint_host,
                                used_client,limit);

                        Thread t = new Thread(token_scan);
                        t.start();

                        scan_threads.add(new Pair<Thread, Token_scan>(t, token_scan));

                    }

                    for (Pair<Thread, Token_scan> scan_thread : scan_threads) {
                        scan_thread.left.join();
                        size += scan_thread.right.retrieved_keys.size();
                    }

                    _scan_table = table;

                } else {

                    if (!_scan_table.equals(table)) {
                        used_client.set_keyspace(table);
                        _scan_table = table;

                    }

                    KeyRange kr = new KeyRange().setStart_key(startkey.getBytes("UTF-8")).setEnd_key(new byte[]{}).setCount(limit);

                    boolean finished = false;

                    while (!finished) {

                        //For memory purposes we choose this way
                        results = new ArrayList<KeySlice>();

                        List<KeySlice> temp_results = used_client.get_range_slices(parent, predicate, kr, scan_ConsistencyLevel);

                        if (temp_results.size() < limit) {
                            finished = true;
                        } else {
                            kr.setStart_key(temp_results.get(temp_results.size() - 1).getKey());
                        }

                        for (KeySlice keySlice : temp_results) {
                            results.add(keySlice);
                        }

                        preform_debug_creation(temp_results);

                        size += results.size();
                    }
                }


                System.out.println("(debug) scan size: " + size + "consitency " + scan_ConsistencyLevel.name());

                if (_debug) {
                    System.out.println("Scanning startkey: " + startkey);
                }

                HashMap<String, ByteIterator> tuple;
                for (KeySlice oneresult : results) {
                    tuple = new HashMap<String, ByteIterator>();

                    Column column;
                    String name;
                    ByteIterator value;
                    for (ColumnOrSuperColumn onecol : oneresult.columns) {
                        column = onecol.column;
                        name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                        value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());

                        tuple.put(name, value);

                        if (_debug) {
                            System.out.print("(" + name + "=" + value + ")");
                        }
                    }

                    result.add(tuple);
                    if (_debug) {
                        System.out.println();
                    }
                }

                return Ok;
            } catch (Exception e) {
                logError("scan",e,selected_host);
                errorexception = e;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        System.out.println("error: "+errorexception.getClass().toString()+" : "+errorexception.getMessage());
        //errorexception.printStackTrace();
        //errorexception.printStackTrace(System.out);
        return Error;
    }

    class Token_scan implements Runnable {

        List<Pair<String, String>> endpoint_token_ranges;
        Cassandra.Client scan_client;
        String client_host;
        int limit;
        int number_retrieved_keys = 0;
        int number_retrieved_files = 0;
        int number_empty_keys = 0;
        List<String> retrieved_keys = new ArrayList<String>();
        SlicePredicate predicate;

        Token_scan(List<Pair<String, String>> endpoint_token_ranges, String client_host, Cassandra.Client scan_client, int limit) {
            this.endpoint_token_ranges = endpoint_token_ranges;
            this.scan_client = scan_client;
            this.limit = limit;
            this.client_host = client_host;
        }

        @Override
        public void run() {

            for (Pair<String, String> token_info : endpoint_token_ranges) {

                predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, column_buffer));
                TreeMap<ByteBuffer,byte[]> column_limits = new TreeMap<ByteBuffer, byte[]>();

                String start_token = token_info.getLeft();
                String end_token = token_info.getRight();

                try {

                    IPartitioner partitioner = null;
                    partitioner = FBUtilities.newPartitioner(scan_client.describe_partitioner());

                    boolean finished = false;

                    KeyRange kr = new KeyRange().setStart_token(start_token).setEnd_token(end_token).setCount(limit);

                    while (!finished) {

                        List<KeySlice> temp_results = scan_client.get_range_slices(parent, predicate, kr, scan_ConsistencyLevel);

                        int number_files = 0;
                        int number_keys = 0;
                        int empty_keys = 0;

                        for (KeySlice keySlice : temp_results) {

                            if (keySlice.getColumnsSize() > 0) {
                                number_keys++;
                                retrieved_keys.add(new String(keySlice.getKey()));
                                number_files+=keySlice.getColumnsSize();
                            } else {
                                empty_keys++;
                            }

                            if(keySlice.getColumnsSize()==column_buffer){
                                List<ColumnOrSuperColumn> columns = keySlice.getColumns();
                                Column last_column = columns.get(columns.size()-1).getColumn();
                                column_limits.put(keySlice.key,last_column.getName());
                            }
                        }

                        number_retrieved_keys += number_keys;
                        number_retrieved_files += number_files;
                        number_empty_keys += empty_keys;

                        if (number_files == 0) {
                            finished = true;

                        } else {
                            KeySlice lastRow = temp_results.get(temp_results.size() - 1);
                            ByteBuffer rowkey = lastRow.key;
                            start_token = partitioner.getTokenFactory().toString(partitioner.getToken(rowkey));
                            kr.setStart_token(start_token);
                        }

                        preform_debug_creation(temp_results);

                    }

                    if(!column_limits.isEmpty()){
                        for(ByteBuffer key : column_limits.keySet()){
                            finished = false;

                            byte[] start_column_name = column_limits.get(key);
                            ByteBuffer row_key = key;

                            predicate.getSlice_range().setStart(start_column_name);
                            while (!finished) {
                                List<ColumnOrSuperColumn> results = scan_client.get_slice(row_key,parent,predicate,scan_ConsistencyLevel);
                                number_retrieved_files += results.size()-1;

                                if(results.size()<column_buffer){
                                    finished =  true;
                                }
                                else{
                                    Column last_column = results.get(results.size()-1).getColumn();
                                    predicate.getSlice_range().setStart(last_column.getName());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logError("Token_scan",e,client_host);
                    System.out.println("error: "+errorexception.getClass().toString()+" : "+errorexception.getMessage());
                }
            }

            System.out.println("(debug:) Thread scan k: " + number_retrieved_keys + " f: "+number_retrieved_files+" e: " + number_empty_keys);

        }
    }

    public void preform_debug_creation(List<KeySlice> keySlices) {

        if (debug_folder == null) {
            return;
        }

        for (KeySlice keySlice : keySlices) {
            String key = new String(keySlice.getKey());
            if (keySlice.getColumnsSize() == 1) {
                Column column = keySlice.getColumns().get(0).getColumn();
                String column_name = new String(column.getName());
                if (column_name.equals("_file")) { //single column mode

                    createFolders(key, true);
                    File file = new File(key);
                    if(!file.exists()){
                        createFile(key,column.getValue());
                    }else{
                        System.out.println("Warn: File already exists");
                    }

                } else { //folder with one column
                    String folder = createFolders(key, false);
                    createFile(folder+"/"+column_name,column.getValue());

                }
            } else {
                String folder = createFolders(key, false);
                for(ColumnOrSuperColumn columnOrSuperColumn :keySlice.getColumns()){
                    Column column = columnOrSuperColumn.getColumn();
                    String column_name = new String(column.getName());
                    createFile(folder+"/"+column_name,column.getValue());
                }
            }
        }
    }

    private String createFolders(String path, boolean ends_with_file) {
       
        String[] folders = path.split("/");
        String base_folder = debug_folder;
        if(!base_folder.endsWith("/")){
            base_folder =  base_folder+"/";
        }

        int folder_length = folders.length;
        folder_length = ends_with_file ?  folder_length -1 : folder_length;

        for (int i = 0; i < folder_length; i++) {
            String folder = folders[i];
            base_folder+="/"+folder;
        }
        
        File new_folders = new File(base_folder);
        if(!new_folders.exists()){
            new_folders.mkdirs();
        }

        return base_folder;
    }

    
    public void createFile(String path,byte[] value)  {
        OutputStream out = null;
        try {
            out = new FileOutputStream(path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try{
            out.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        if (!_table.equals(table)) {
            try {
                client.set_keyspace(table);
                _table = table;
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++) {
            if (_debug) {
                System.out.println("Inserting key: " + key);
            }

            try {
                ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

                Column col;
                ColumnOrSuperColumn column;
                for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                    col = new Column();
                    col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
                    col.setValue(ByteBuffer.wrap(entry.getValue().toArray()));
                    col.setTimestamp(System.currentTimeMillis());

                    column = new ColumnOrSuperColumn();
                    column.setColumn(col);

                    mutations.add(new Mutation().setColumn_or_supercolumn(column));
                }

                mutationMap.put(column_family, mutations);
                record.put(wrappedKey, mutationMap);

                client.batch_mutate(record, ConsistencyLevel.ONE);

                mutations.clear();
                mutationMap.clear();
                record.clear();

                return Ok;
            } catch (Exception e) {
                errorexception = e;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }

        errorexception.printStackTrace();
        errorexception.printStackTrace(System.out);
        return Error;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key) {
        if (!_table.equals(table)) {
            try {
                client.set_keyspace(table);
                _table = table;
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++) {
            try {
                client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
                        new ColumnPath(column_family),
                        System.currentTimeMillis(),
                        ConsistencyLevel.ONE);

                if (_debug) {
                    System.out.println("Delete key: " + key);
                }

                return Ok;
            } catch (Exception e) {
                errorexception = e;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        errorexception.printStackTrace();
        errorexception.printStackTrace(System.out);
        return Error;
    }


    public static void main(String[] args) {
        
        String base_folder = "/Users/pedrogomes/Desktop/Work/eurotux/test_folder_creation";


        CassandraClient8 cli = new CassandraClient8();
        cli.debug_folder = base_folder;
        cli.createFolders("xpto1/cenas2/teste/1",false);
//
//        Properties props = new Properties();
//
//        //props.setProperty("hosts", args[0]);
//        props.setProperty("hosts", "192.168.111.221");
//        props.setProperty("cassandra.scan_connections", "192.168.111.224,192.168.111.228,192.168.111.232");
//
//        cli.setProperties(props);
//
//        try {
//            cli.init();
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(0);
//        }
//
//        try {
//
//
//            List<TokenRange> tr = cli.client.describe_ring("Eurotux");
//
//            try {
//                IPartitioner partitioner = FBUtilities.newPartitioner(cli.client.describe_partitioner());
//            } catch (ConfigurationException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//            }
//
//            int number_slices = 0;
//            for (TokenRange trange : tr) {
//                System.out.print(trange.getStart_token());
//                System.out.print(" - " + trange.getEnd_token());
//                boolean found = false;
//                String host = "";
//                for (EndpointDetails epd : trange.getEndpoint_details()) {
//                    if (epd.getDatacenter().equals("DC2")) {
//                        System.out.println(" : " + epd.getHost());
//                        host = epd.getHost();
//                        found = true;
//                    }
//                }
//                if (!found) {
//                    System.out.println("Endpoint without DC2");
//                }
//
//                cli.scan_clients.get(host).set_keyspace("Eurotux");
//
//                List<String> splits = cli.scan_clients.get(host).describe_splits("File_system", trange.getStart_token(), trange.getEnd_token(), 2000);
//                System.out.println(splits.toString());
//                number_slices += splits.size() - 1;
//            }
//
//            System.out.println("Number of splits: " + (number_slices));
//
//        } catch (InvalidRequestException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } catch (TException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//
//        HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
//        vals.put("age", new StringByteIterator("57"));
//        vals.put("middlename", new StringByteIterator("bradley"));
//        vals.put("favoritecolor", new StringByteIterator("blue"));
//        int res = cli.insert("usertable", "BrianFrankCooper", vals);
//        System.out.println("Result of insert: " + res);
//
//        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
//        HashSet<String> fields = new HashSet<String>();
//        fields.add("middlename");
//        fields.add("age");
//        fields.add("favoritecolor");
//        res = cli.read("usertable", "BrianFrankCooper", null, result);
//        System.out.println("Result of read: " + res);
//        for (String s : result.keySet()) {
//            System.out.println("[" + s + "]=[" + result.get(s) + "]");
//        }
//
//        res = cli.delete("usertable", "BrianFrankCooper");
//        System.out.println("Result of delete: " + res);
        
        
    }


}
