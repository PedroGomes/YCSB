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

import java.util.*;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.cassandra.thrift.*;


//XXXX if we do replication, fix the consistency levels

/**
 * Cassandra 0.8 client for YCSB framework
 */
public class CassandraClient8 extends DB {
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

    public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
    public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

    public static final String SCAN_CONNECTIONS_PROPERTY = "cassandra.scan_connections";

    public static final String TOKENIZED_SCANS_PROPERTY = "cassandra.tokenized_scans";


    TTransport tr;
    Cassandra.Client client;

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
    ConsistencyLevel read_ConsistencyLevel = ConsistencyLevel.QUORUM;
    ConsistencyLevel write_ConsistencyLevel = ConsistencyLevel.QUORUM;
    ConsistencyLevel scan_ConsistencyLevel = ConsistencyLevel.QUORUM;


    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void init() throws DBException {
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

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        row_buffer = Integer.parseInt(getProperties().getProperty(ROW_BUFFER_PROPERTY, "1000"));

        try {

            read_ConsistencyLevel = getConsistencyLevel(getProperties().getProperty(READ_CONSISTENCY_PROPERTY, "QUORUM"));
            write_ConsistencyLevel = getConsistencyLevel(getProperties().getProperty(WRITE_CONSISTENCY_PROPERTY, "QUORUM"));
            scan_ConsistencyLevel = getConsistencyLevel(getProperties().getProperty(SCAN_CONSISTENCY_PROPERTY, "ONE"));
        } catch (Exception e) {
            throw new DBException(e);
        }

        _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        String[] allhosts = hosts.split(",");
        String myhost = allhosts[random.nextInt(allhosts.length)];
        //System.out.println(java.util.Arrays.toString(allhosts));
        Pair<Cassandra.Client, TTransport> client_connection = makeConnection(username, password, myhost);
        client = client_connection.getLeft();
        tr = client_connection.getRight();

        //System.out.println("(debug:) client connection to "+myhost+" RCL: "+read_ConsistencyLevel);

        scan_cons_trp = new TreeMap<String, TTransport>();
        scan_clients = new TreeMap<String, Cassandra.Client>();

        String scan_hosts = getProperties().getProperty(SCAN_CONNECTIONS_PROPERTY);
        if (scan_hosts != null && !scan_hosts.trim().isEmpty()) {
            using_scan_connection = true;
            allhosts = scan_hosts.split(",");
            for (String cass_scan_host : allhosts) {
                client_connection = makeConnection(username, password, cass_scan_host);
                scan_clients.put(cass_scan_host, client_connection.getLeft());
                scan_cons_trp.put(cass_scan_host, client_connection.getRight());
            }

        }

        tokenized_scans = Boolean.parseBoolean(getProperties().getProperty(TOKENIZED_SCANS_PROPERTY, "false"));

        //  System.out.println("(debug:) scan connection to "+myhost+" SCL: "+scan_ConsistencyLevel);

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
        tr.close();
        if (using_scan_connection) {
            for (TTransport scan_con_trp : scan_cons_trp.values())
                scan_con_trp.close();
        }
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

        for (int i = 0; i < OperationRetries; i++) {

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
        if (using_scan_connection) {
            int selected = random.nextInt(scan_clients.size());
            int i = 0;
            for (String host : scan_clients.keySet()) {
                if (i == selected)
                    used_client = scan_clients.get(host);
                i++;
            }
        } else {
            used_client = client;
        }

        for (int i = 0; i < OperationRetries; i++) {

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

                    List<Pair<Thread,Token_scan>> scan_threads = new LinkedList<Pair<Thread, Token_scan>>();
                    
                    for (String endpoint_host : token_endpoints.keySet()) {

                        used_client = scan_clients.get(endpoint_host);
                        if (!_scan_table.equals(table)) {
                            used_client.set_keyspace(table);
                        }

                        Token_scan  token_scan = new Token_scan(token_endpoints.get(endpoint_host),used_client,
                                limit,predicate);

                        Thread t  = new Thread(token_scan);
                        t.start();

                        scan_threads.add(new Pair<Thread, Token_scan>(t,token_scan));

                    }

                    for(Pair<Thread,Token_scan> scan_thread : scan_threads){
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

    class Token_scan implements Runnable {

        List<Pair<String, String>> endpoint_token_ranges;
        Cassandra.Client scan_client;
        int limit;
        int number_retrieved_keys = 0;
        int number_empty_keys = 0;
        List<String> retrieved_keys = new ArrayList<String>();
        SlicePredicate predicate;

        Token_scan(List<Pair<String, String>> endpoint_token_ranges, Cassandra.Client scan_client, int limit, SlicePredicate predicate) {
            this.endpoint_token_ranges = endpoint_token_ranges;
            this.scan_client = scan_client;
            this.limit = limit;
            this.predicate = predicate;
        }

        @Override
        public void run() {

            for (Pair<String, String> token_info : endpoint_token_ranges) {

                String start_token = token_info.getLeft();
                String end_token = token_info.getRight();

                try {

                    IPartitioner partitioner = null;
                    partitioner = FBUtilities.newPartitioner(scan_client.describe_partitioner());

                    boolean finished = false;

                    KeyRange kr = new KeyRange().setStart_token(start_token).setEnd_token(end_token).setCount(limit);

                    while (!finished) {

                        List<KeySlice> temp_results = scan_client.get_range_slices(parent, predicate, kr, scan_ConsistencyLevel);

                        int number_keys = 0;
                        int empty_keys = 0;

                        for (KeySlice keySlice : temp_results) {
                            number_keys++;
                            if (keySlice.getColumnsSize() > 0) {
                                retrieved_keys.add(new String(keySlice.getKey()));
                            }
                            else {
                                empty_keys++;
                            }
                        }

                        number_retrieved_keys += number_keys;
                        number_empty_keys += empty_keys;

                        if (number_keys == 0) {
                            finished = true;
                            
                        } else {
                            KeySlice lastRow = temp_results.get(temp_results.size() - 1);
                            ByteBuffer rowkey = lastRow.key;
                            start_token = partitioner.getTokenFactory().toString(partitioner.getToken(rowkey));
                            kr.setStart_token(start_token);
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            System.out.println("(debug:) Thread scan t: "+number_retrieved_keys+" e: "+number_empty_keys);

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
        CassandraClient8 cli = new CassandraClient8();

        Properties props = new Properties();

        //props.setProperty("hosts", args[0]);
        props.setProperty("hosts", "192.168.111.221");
        props.setProperty("cassandra.scan_connections", "192.168.111.224,192.168.111.228,192.168.111.232");

        cli.setProperties(props);

        try {
            cli.init();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        try {


            List<TokenRange> tr = cli.client.describe_ring("Eurotux");

            try {
                IPartitioner partitioner = FBUtilities.newPartitioner(cli.client.describe_partitioner());
            } catch (ConfigurationException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            int number_slices = 0;
            for (TokenRange trange : tr) {
                System.out.print(trange.getStart_token());
                System.out.print(" - " + trange.getEnd_token());
                boolean found = false;
                String host = "";
                for (EndpointDetails epd : trange.getEndpoint_details()) {
                    if (epd.getDatacenter().equals("DC2")) {
                        System.out.println(" : " + epd.getHost());
                        host = epd.getHost();
                        found = true;
                    }
                }
                if (!found) {
                    System.out.println("Endpoint without DC2");
                }

                cli.scan_clients.get(host).set_keyspace("Eurotux");

                List<String> splits = cli.scan_clients.get(host).describe_splits("File_system", trange.getStart_token(), trange.getEnd_token(), 2000);
                System.out.println(splits.toString());
                number_slices += splits.size() - 1;
            }

            System.out.println("Number of splits: " + (number_slices));

        } catch (InvalidRequestException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (TException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
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
