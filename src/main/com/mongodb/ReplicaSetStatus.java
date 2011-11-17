// ReplicaSetStatus.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.util.JSON;

/**
 * keeps replica set status
 * has a background thread to ping so it stays current
 *
 * TODO
 *  pull config to get
 *      priority
 *      slave delay
 *      tags (when we do it)
 */
public class ReplicaSetStatus {

	static final Logger _rootLogger = Logger.getLogger( "com.mongodb.ReplicaSetStatus" );
    static final int UNAUTHENTICATED_ERROR_CODE = 10057;

    ReplicaSetStatus( Mongo mongo, List<ServerAddress> initial ){
        _mongoOptions = _mongoOptionsDefaults.copy();
        _mongoOptions.socketFactory = mongo._options.socketFactory;

        _mongo = mongo;
        _all = Collections.synchronizedList( new ArrayList<Node>() );
        for ( ServerAddress addr : initial ){
            _all.add( new Node( addr ) );
        }
        _nextResolveTime = System.currentTimeMillis() + inetAddrCacheMS;

        _updater = new Updater();
        _secondaryStrategy = useNoQueueSecondarySelection ? new NoQueueStrategy(slaveAcceptableLatencyMS, 10) 
                                : new DefaultReplicaSetSecondaryStrategy(slaveAcceptableLatencyMS);
    }

    void start() {
        _updater.start();
    }

    boolean ready(){
        return _setName != null;
    }

    public String getName() {
        return _setName;
    }

    @Override
	public String toString() {
	StringBuffer sb = new StringBuffer();
	sb.append("{replSetName: '" + _setName );
	sb.append("', closed:").append(_closed).append(", ");
	sb.append("nextResolveTime:'").append(new Date(_nextResolveTime).toString()).append("', ");
	sb.append("members : [ ");
	if(_all != null) {
		for(Node n : _all)
			sb.append(n.toJSON()).append(",");
		sb.setLength(sb.length()-1); //remove last comma
	}
	sb.append("] ");

	return sb.toString();
	}

    void _checkClosed(){
        if ( _closed )
            throw new IllegalStateException( "ReplicaSetStatus closed" );
    }

    /**
     * @return master or null if don't have one
     */
    public ServerAddress getMaster(){
        Node n = getMasterNode();
        if ( n == null )
            return null;
        return n._addr;
    }

    Node getMasterNode(){
        _checkClosed();
        for ( int i=0; i<_all.size(); i++ ){
            Node n = _all.get(i);
            if ( n.master() )
                return n;
        }
        return null;
    }

	/**
	 * @param srv
	 *            the server to compare
	 * @return indication if the ServerAddress is the current Master/Primary
	 */
	public boolean isMaster(ServerAddress srv) {
		if (srv == null)
			return false;

		return srv.equals(getMaster());
	}

    /**
     * @return a good secondary by tag value or null if can't find one
     */
    public ServerAddress getASecondary( DBObject tags ){
        for ( String key : tags.keySet() ) {
            ServerAddress secondary = getASecondary( key, tags.get( key ).toString() );
            if (secondary != null)
                return secondary;
        }
        // No matching server for any supplied tags was found
        return null;
    }

    /**
     * @return a good secondary or null if can't find one
     */
    public ServerAddress getASecondary(){
        return getASecondary( null, null );
    }
    /**
     * @return a good secondary or null if can't find one
     */
    public ServerAddress getASecondary( String tagKey, String tagValue ) {
        _checkClosed();
        Node best = _secondaryStrategy.select(tagKey, tagValue, _all);
        return ( best != null ) ? best._addr : null;
    }
    
    boolean hasServerUp() {
        for (int i = 0; i < _all.size(); i++) {
            Node n = _all.get(i);
            if (n._ok) {
                return true;
            }
        }
        return false;
    }

    /**
     * The replica set node object.
     */
    public class Node implements ReplicaSetNode {

        Node( ServerAddress addr ){
            _addr = addr;
            _port = new DBPort( addr , null , _mongoOptions );
            _names.add( addr.toString() );
        }

        private void updateAddr() {
            try {
                if (_addr.updateInetAddr()) {
                    // address changed, need to use new ports
                    _port = new DBPort(_addr, null, _mongoOptions);
                    _mongo.getConnector().updatePortPool(_addr);
                    _logger.log(Level.INFO, "Address of host " + _addr.toString() + " changed to " + _addr.getSocketAddress().toString());
                }
            } catch (UnknownHostException ex) {
                _logger.log(Level.WARNING, null, ex);
            }
        }

        synchronized void update(){
            update(null);
        }

        synchronized void update(Set<Node> seenNodes){
            try {
                long start = System.currentTimeMillis();
                CommandResult res = _port.runCommand( _mongo.getDB("admin") , _serverStatusCommand );
                boolean first = (_lastCheck == 0);
                _lastCheck = System.currentTimeMillis();
                float newPing = _lastCheck - start;
                if (first)
                    _pingTime = newPing;
                else
                    _pingTime = _pingTime + ((newPing - _pingTime) / latencySmoothFactor);
                _rootLogger.log( Level.FINE , "Latency to " + _addr + " actual=" + newPing + " smoothed=" + _pingTime );

                if ( res == null ){
                    throw new MongoInternalException("Invalid null value returned from serverStatus");
                }

                BasicDBObject replRes = (BasicDBObject)res.get("repl");
                if ( replRes == null ) {
                    // TODO(jon) is this safe? maybe not for legacy master-slave or replica-pairs? 
                    // default to master if not a repl set
                    replRes = new BasicDBObject("ismaster", true);
                }

                if (!_ok) {
                    _logger.log( Level.INFO , "Server seen up: " + _addr );
                }
                
                if ( res.containsField("globalLock") ) {
                    _queueSize = ((BasicDBObject)((BasicDBObject)res.get("globalLock")).get("currentQueue")).getInt("total");
                }

                _ok = true;
                _isMaster = replRes.getBoolean( "ismaster" , false );
                _isSecondary = replRes.getBoolean( "secondary" , false );
                _lastPrimarySignal = replRes.getString( "primary" );

                if ( replRes.containsField( "hosts" ) ){
                    for ( Object x : (List)replRes.get("hosts") ){
                        String host = x.toString();
                        Node node = _addIfNotHere(host);
                        if (node != null && seenNodes != null)
                            seenNodes.add(node);
                    }
                }

                if ( replRes.containsField( "passives" ) ){
                    for ( Object x : (List)replRes.get("passives") ){
                        String host = x.toString();
                        Node node = _addIfNotHere(host);
                        if (node != null && seenNodes != null)
                            seenNodes.add(node);
                    }
                }

                // Tags were added in 2.0 but may not be present
                if (replRes.containsField( "tags" )) {
                    DBObject tags = (DBObject) replRes.get( "tags" );
                    for ( String key : tags.keySet() ) {
                        _tags.put( key, tags.get( key ).toString() );
                    }
                }

                if (_isMaster ) {
                    // max size was added in 1.8
                    if (replRes.containsField("maxBsonObjectSize"))
                        maxBsonObjectSize = ((Integer)replRes.get( "maxBsonObjectSize" )).intValue();
                    else
                        maxBsonObjectSize = Bytes.MAX_OBJECT_SIZE;
                }

                if (replRes.containsField("setName")) {
	                String setName = replRes.get( "setName" ).toString();
	                if ( _setName == null ){
	                    _setName = setName;
	                    _logger = Logger.getLogger( _rootLogger.getName() + "." + setName );
	                }
	                else if ( !_setName.equals( setName ) ){
	                    _logger.log( Level.SEVERE , "mis match set name old: " + _setName + " new: " + setName );
	                    return;
	                }
                }

            }
            catch ( Exception e ){
                if (_ok == true) {
                    _logger.log( Level.WARNING , "Server seen down: " + _addr, e );
                } else if (Math.random() < 0.1) {
                    _logger.log( Level.WARNING , "Server seen down: " + _addr );
                }
                _ok = false;
            }

            if ( ! _isMaster )
                return;
        }

        public boolean master(){
            return _ok && _isMaster;
        }

        public boolean secondary(){
            return _ok && _isSecondary;
        }

        public boolean checkTag(String key, String value){
            return _tags.containsKey( key ) && _tags.get( key ).equals( value );
        }

        public String toString(){
            StringBuilder buf = new StringBuilder();
            buf.append( "Replica Set Node: " ).append( _addr ).append( "\n" );
            buf.append( "\t ok \t" ).append( _ok ).append( "\n" );
            buf.append( "\t ping \t" ).append( _pingTime ).append( "\n" );
            buf.append( "\t queueSize \t" ).append(_queueSize).append( "\n" );
            buf.append( "\t master \t" ).append( _isMaster ).append( "\n" );
            buf.append( "\t secondary \t" ).append( _isSecondary ).append( "\n" );

            buf.append( "\t priority \t" ).append( _priority ).append( "\n" );

            buf.append( "\t tags \t" ).append( JSON.serialize( _tags )  ).append( "\n" );

            return buf.toString();
        }

        public String toJSON(){
            StringBuilder buf = new StringBuilder();
            buf.append( "{ address:'" ).append( _addr ).append( "', " );
            buf.append( "ok:" ).append( _ok ).append( ", " );
            buf.append( "ping:" ).append( _pingTime ).append( ", " );
            buf.append( "isMaster:" ).append( _isMaster ).append( ", " );
            buf.append( "isSecondary:" ).append( _isSecondary ).append( ", " );
            buf.append( "priority:" ).append( _priority ).append( ", " );
            if(_tags != null && _tags.size() > 0)
		buf.append( "tags:" ).append( JSON.serialize( _tags )  );
            buf.append("}");

            return buf.toString();
        }
        
        public DBPort getPort() {
            return _port;
        }

        public void close() {
            _port.close();
            _port = null;
        }
        
        public float getPingTime() {
            return _pingTime;
        }
        
        @Override
        public int getQueueSize() {
            return _queueSize;
        }

        final ServerAddress _addr;
        final Set<String> _names = Collections.synchronizedSet( new HashSet<String>() );
        DBPort _port; // we have our own port so we can set different socket options and don't have to owrry about the pool
        final LinkedHashMap<String, String> _tags = new LinkedHashMap<String, String>( );

        boolean _ok = false;
        long _lastCheck = 0;
        float _pingTime = 0;

        boolean _isMaster = false;
        boolean _isSecondary = false;

        double _priority = 0;
        int _queueSize = 0;

    }

    class Updater extends Thread {
        Updater(){
            super( "ReplicaSetStatus:Updater" );
            setDaemon( true );
        }

        public void run(){
            while ( ! _closed ){
                try {
                    updateAll();

                    long now = System.currentTimeMillis();
                    if (inetAddrCacheMS > 0 && _nextResolveTime < now) {
                        _nextResolveTime = now + inetAddrCacheMS;
                        for (Node node : _all) {
                            node.updateAddr();
                        }
                    }

                    // force check on master
                    // otherwise master change may go unnoticed for a while if no write concern
                    _mongo.getConnector().checkMaster(true, false);
                }
                catch ( Exception e ){
                    _logger.log( Level.WARNING , "couldn't do update pass" , e );
                }

                try {
                    Thread.sleep( updaterIntervalMS );
                }
                catch ( InterruptedException ie ){
                }

            }
        }
    }

    Node ensureMaster(){
        Node n = getMasterNode();
        if ( n != null ){
            n.update();
            if ( n._isMaster )
                return n;
        }

        if ( _lastPrimarySignal != null ){
            n = findNode( _lastPrimarySignal );
            if (n != null) {
                n.update();
                if ( n._isMaster )
                    return n;
            }
        }

        updateAll();
        return getMasterNode();
    }

    synchronized void updateAll(){
        HashSet<Node> seenNodes = new HashSet<Node>();
        for ( int i=0; i<_all.size(); i++ ){
            Node n = _all.get(i);
            n.update(seenNodes);
        }

        if (seenNodes.size() > 0) {
            // not empty, means that at least 1 server gave node list
            // remove unused hosts
            Iterator<Node> it = _all.iterator();
            while (it.hasNext()) {
                if (!seenNodes.contains(it.next()))
                    it.remove();
            }
        }
    }

    public List<ServerAddress> getServerAddressList() {
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        for (Node node : _all)
            addrs.add(node._addr);
        return addrs;
    }

    Node _addIfNotHere( String host ){
        Node n = findNode( host );
        if ( n == null ){
            try {
                n = new Node( new ServerAddress( host ) );
                _all.add( n );
            }
            catch ( UnknownHostException un ){
                _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            }
        }
        return n;
    }

    public Node findNode( String host ){
        for ( int i=0; i<_all.size(); i++ )
            if ( _all.get(i)._names.contains( host ) )
                return _all.get(i);

        ServerAddress addr = null;
        try {
            addr = new ServerAddress( host );
        }
        catch ( UnknownHostException un ){
            _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            return null;
        }

        for ( int i=0; i<_all.size(); i++ ){
            if ( _all.get(i)._addr.equals( addr ) ){
                _all.get(i)._names.add( host );
                return _all.get(i);
            }
        }

        return null;
    }

    void printStatus(){
        for ( int i=0; i<_all.size(); i++ )
            System.out.println( _all.get(i) );
    }

    void close(){
        if (!_closed) {
            _closed = true;
            for (int i = 0; i < _all.size(); i++) {
                _all.get(i).close();
            }
        }
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * @return the maximum size, or 0 if not obtained from servers yet.
     */
    public int getMaxBsonObjectSize() {
        return maxBsonObjectSize;
    }
    
    public List<Node> getAll() {
        return new ArrayList<Node>(_all);
    }

    final List<Node> _all;
    Updater _updater;
    Mongo _mongo;
    String _setName = null; // null until init
    int maxBsonObjectSize = 0;
    Logger _logger = _rootLogger; // will get changed to use set name once its found

    String _lastPrimarySignal;
    boolean _closed = false;

    
    final ReplicaSetSecondaryStrategy _secondaryStrategy;
    long _nextResolveTime;

    static int updaterIntervalMS;
    static int slaveAcceptableLatencyMS;
    static int inetAddrCacheMS;
    static float latencySmoothFactor;
    static boolean useNoQueueSecondarySelection;

    final MongoOptions _mongoOptions;
    static final MongoOptions _mongoOptionsDefaults = new MongoOptions();

    static {
        updaterIntervalMS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        slaveAcceptableLatencyMS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        inetAddrCacheMS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));
        latencySmoothFactor = Float.parseFloat(System.getProperty("com.mongodb.latencySmoothFactor", "4"));
        useNoQueueSecondarySelection = Boolean.parseBoolean(System.getProperty("com.mongodb.noQueueSecondarySelection"));
        _mongoOptionsDefaults.connectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
        _mongoOptionsDefaults.socketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
    }

    private static final DBObject _serverStatusCommand = new BasicDBObject( "serverStatus" , 1 );

    public static void main( String args[] )
        throws Exception {
        List<ServerAddress> addrs = new LinkedList<ServerAddress>();
        addrs.add( new ServerAddress( "127.0.0.1" , 27018 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27019 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27020 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27021 ) );

        Mongo m = new Mongo( addrs );

        ReplicaSetStatus status = new ReplicaSetStatus( m, addrs );
        status.start();
        System.out.println( status.ensureMaster()._addr );

        while ( true ){
            System.out.println( status.ready() );
            if ( status.ready() ){
                status.printStatus();
                System.out.println( "master: " + status.getMaster() + "\t secondary: " + status.getASecondary() );
            }
            System.out.println( "-----------------------" );
            DBObject tags = new BasicDBObject();
            tags.put( "dc", "newyork" );
            System.out.println( "Tagged Node: " + status.getASecondary( tags ) );
            Thread.sleep( 5000 );
        }

    }
}
