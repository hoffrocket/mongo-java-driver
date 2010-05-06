// ByteEncoder.java

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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import com.mongodb.ByteDecoder.UseByteDecoder;
import com.mongodb.util.Pool;
import com.mongodb.util.Pool.PoolFactory;
import com.mongodb.util.Pool.UsePooled;

/** 
 * Serializes a <code>DBObject</code> into a string that can be sent to the database.
 * <p>There is a pool of available encoders.  Create a new one as follows:
 * <blockquote></pre>
 *     ByteEncoder encoder = ByteEncoder.get(); // try forever until an encoder is available
 * </pre></blockquote>
 *
 * <p>Each key/value pair in the <code>DBObject</code> is encoded in the following format:
 * <blockquote>
 * <i>&lt;type (</i><code>byte</code><i>)&gt;&lt;name (</i><code>String</code></i>)&gt;&lt;0 (</i><code>byte</code><i>)&gt;&lt;data (serialized data)&gt;</i>
 * </blockquote>
 *
 * For example:
 * <blockquote>
 *   &lt;<code>NUMBER</code>&gt;&lt;name&gt;0&lt;double&gt; <code>// NUMBER = 1</code><br />
 *   &lt;<code>STRING</code>&gt;&lt;name&gt;0&lt;len&gt;&lt;string&gt;0 <code>// STRING = 2</code>
 * </blockquote>
 */
public class ByteEncoder extends Bytes {

    static final boolean DEBUG = Boolean.getBoolean( "DEBUG.BE" );
    
	static class ByteEncoderFactory implements PoolFactory<ByteEncoder> {

		@Override
		public ByteEncoder create() {
			if ( D ) System.out.println( "creating new ByteEncoder" );
            return new ByteEncoder();
		}

		@Override
		public void reset(ByteEncoder obj) {
			obj.reset();
		}
	}


    
	interface UseByteEncoder<R> extends UsePooled<R, ByteEncoder> {
		public R use(ByteEncoder encoder) throws Exception;
	}
	
	static abstract class VoidUseByteEncoder implements UseByteEncoder<Void> {
		@Override
		final public Void use(ByteEncoder encoder) throws Exception {
			u(encoder);
			return null;
		}
		
		abstract protected void u(ByteEncoder encoder) throws Exception;
	}
	
	
	public static <R> R use(UseByteEncoder<R> ube) {
		return _pool.use(ube);
	}
	

	private final static Pool<ByteEncoder> _pool = new Pool<ByteEncoder>(new ByteEncoderFactory(), NUM_ENCODERS, NUM_ENCODERS*2);
    
    
    // ----
    
    private ByteEncoder(){
        _buf = ByteBuffer.allocateDirect( MAX_OBJECT_SIZE + 2048 );
        _buf.order( Bytes.ORDER );
    }

    /**
     *  Returns the bytes in the bytebuffer.  Attempts to leave the
     *  bytebuffer in the same state.  Note that mark, if set, is lost.
     *
     * @return  array of bytes
     */
    public byte[] getBytes() {

        int pos = _buf.position();
        int limit = _buf.limit();

        flip();

        byte[] arr = new byte[_buf.limit()];

        _buf.get(arr);

        flip();

        _buf.position(pos);
        _buf.limit(limit);

        return arr;
    }

    /**
     *  Returns encoder to its starting state, ready to encode an object.
     */
    protected void reset(){
        _buf.position( 0 );
        _buf.limit( _buf.capacity() );
        _flipped = false;
    }

    /**
     *  Switches the encoder from being write-only to being read-only.
     */
    protected void flip(){
        _buf.flip();
        _flipped = true;
    }
    
    /** Encodes a <code>DBObject</code>.
     * This is for the higher level api calls
     * @param o the object to encode
     * @return the number of characters in the encoding
     */
    public int putObject( DBObject o ){
        try {
            return putObject( null , o );
        }
        catch ( BufferOverflowException bof ){
            reset();
            throw getTooLargeException();
        }
    }

    RuntimeException getTooLargeException(){
        return new IllegalArgumentException( "tried to save too large of an object.  " + 
                                             " max size : " + ( _buf.capacity() / 2  ) );
    }

    /**
     * this is really for embedded objects
     */
    int putObject( String name , DBObject o ){
        if ( o == null )
            throw new NullPointerException( "can't save a null object" );

        if ( DEBUG ) System.out.println( "putObject : " + name + " [" + o.getClass() + "]" + " # keys " + o.keySet().size() );
        
        if ( _flipped )
            throw new IllegalStateException( "already flipped" );
        final int start = _buf.position();
        
        byte myType = OBJECT;
        if ( o instanceof List )
            myType = ARRAY;

        if ( _handleSpecialObjects( name , o ) )
            return _buf.position() - start;
        
        if ( name != null ){
            _put( myType , name );
        }

        final int sizePos = _buf.position();
        _buf.putInt( 0 ); // leaving space for this.  set it at the end

        List transientFields = null;
        boolean rewriteID = myType == OBJECT && name == null;
        

        if ( myType == OBJECT ) {
            if ( rewriteID && o.containsField( "_id" ) )
                _putObjectField( "_id" , o.get( "_id" ) );
            
            {
                Object temp = o.get( "_transientFields" );
                if ( temp instanceof List )
                    transientFields = (List)temp;
            }
        }
        

        for ( String s : o.keySet() ){

            if ( rewriteID && s.equals( "_id" ) )
                continue;
            
            if ( transientFields != null && transientFields.contains( s ) )
                continue;
            
            Object val = o.get( s );

            _putObjectField( s , val );

        }
        _buf.put( EOO );
        
        _buf.putInt( sizePos , _buf.position() - sizePos );
        return _buf.position() - start;
    }

    private void _putObjectField( String name , Object val ){

        if ( name.equals( "_transientFields" ) )
            return;
        
        if ( DEBUG ) System.out.println( "\t put thing : " + name );
        
        if ( name.equals( "$where") && val instanceof String ){
            _put( CODE , name );
            _putValueString( val.toString() );
            return;
        }
        
        val = Bytes.applyEncodingHooks( val );

        if ( val == null )
            putNull(name);
        else if ( val instanceof Date )
            putDate( name , (Date)val );
        else if ( val instanceof Number )
            putNumber(name, (Number)val );
        else if ( val instanceof String )
            putString(name, val.toString() );
        else if ( val instanceof ObjectId )
            putObjectId(name, (ObjectId)val );
        else if ( val instanceof DBObject )
            putObject(name, (DBObject)val );
        else if ( val instanceof Boolean )
            putBoolean(name, (Boolean)val );
        else if ( val instanceof Pattern )
            putPattern(name, (Pattern)val );
        else if ( val instanceof Map )
            putMap( name , (Map)val );

        else if ( val instanceof List )
            putList( name , (List)val );
        else if ( val instanceof byte[] )
            putBinary( name , (byte[])val );
        else if ( val instanceof Binary )
            putBinary( name , (Binary)val );
        else if ( val.getClass().isArray() )
            putList( name , Arrays.asList( (Object[])val ) );

        else if (val instanceof DBPointer) {

            // temporary - there's the notion of "special object" , but for simple level 0...
            DBPointer r = (DBPointer) val;
            putDBPointer( name , r._ns , (ObjectId)r._id );
        }
        else if (val instanceof DBRefBase ) {
            putDBRef( name, (DBRefBase)val );
        }
        else if (val instanceof Symbol) {
            putSymbol(name, (Symbol) val);
        }
        else if (val instanceof BSONTimestamp) {
            putTimestamp( name , (BSONTimestamp)val );
        }
        else if (val instanceof CodeWScope) {
            putCodeWScope( name , (CodeWScope)val );
        }
        else 
            throw new IllegalArgumentException( "can't serialize " + val.getClass() );
        
    }

    private void putList( String name , List l ){
        _put( ARRAY , name );
        final int sizePos = _buf.position();
        _buf.putInt( 0 );
        
        for ( int i=0; i<l.size(); i++ )
            _putObjectField( String.valueOf( i ) , l.get( i ) );

        _buf.put( EOO );
        _buf.putInt( sizePos , _buf.position() - sizePos );        
    }
    
    private void putMap( String name , Map m ){
        _put( OBJECT , name );
        final int sizePos = _buf.position();
        _buf.putInt( 0 );
        
        for ( Map.Entry entry : (Set<Map.Entry>)m.entrySet() )
            _putObjectField( entry.getKey().toString() , entry.getValue() );

        _buf.put( EOO );
        _buf.putInt( sizePos , _buf.position() - sizePos );
    }
    

    private boolean _handleSpecialObjects( String name , DBObject o ){
        
        if ( o == null )
            return false;

        if ( o instanceof DBCollection ){
            DBCollection c = (DBCollection)o;
            putDBPointer( name , c.getName() , Bytes.COLLECTION_REF_ID );
            return true;
        }
        
        if ( name != null && o instanceof DBPointer ){
            DBPointer r = (DBPointer)o;
            putDBPointer( name , r._ns , (ObjectId)r._id );
            return true;
        }
        
        return false;
    }

    protected int putNull( String name ){
        int start = _buf.position();
        _put( NULL , name );
        return _buf.position() - start;
    }

    protected int putUndefined(String name){
        int start = _buf.position();
        _put(UNDEFINED, name);
        return _buf.position() - start;
    }

    protected int putTimestamp(String name, BSONTimestamp ts ){
        int start = _buf.position();
        _put( TIMESTAMP , name );
        _buf.putInt( ts.getInc() );
        _buf.putInt( ts.getTime() );
        return _buf.position() - start;        
    }

    protected int putCodeWScope( String name , CodeWScope code ){
        final int start = _buf.position();
        _put( CODE_W_SCOPE , name );
        int temp = _buf.position();
        _buf.putInt( 0 );
        _putValueString( code.getCode() );
        putObject( (DBObject)(code.getScope()) );
        _buf.putInt( temp , _buf.position() - temp );
        return _buf.position() - start;        
    }

    protected int putBoolean( String name , Boolean b ){
        int start = _buf.position();
        _put( BOOLEAN , name );
        _buf.put( b ? (byte)0x1 : (byte)0x0 );
        return _buf.position() - start;
    }

    protected int putDate( String name , Date d ){
        int start = _buf.position();
        _put( DATE , name );
        _buf.putLong( d.getTime() );
        return _buf.position() - start;
    }

    protected int putNumber( String name , Number n ){
        int start = _buf.position();
	if ( n instanceof Integer ||
             n instanceof Short ||
             n instanceof Byte ||
             n instanceof AtomicInteger ){
	    _put( NUMBER_INT , name );
	    _buf.putInt( n.intValue() );
	}
        else if ( n instanceof Long || 
                  n instanceof AtomicLong ) {
            _put( NUMBER_LONG , name );
            _buf.putLong( n.longValue() );
        }
	else {
	    _put( NUMBER , name );
	    _buf.putDouble( n.doubleValue() );
	}
        return _buf.position() - start;
    }

    protected void putBinary( String name , byte[] data ){
        
        _put( BINARY , name );
        _buf.putInt( 4 + data.length );

        _buf.put( B_BINARY );
        _buf.putInt( data.length );
        int before = _buf.position();
        _buf.put( data );
        int after = _buf.position();
        
        com.mongodb.util.MyAsserts.assertEquals( after - before , data.length );
    }

    protected void putBinary( String name , Binary val ){
        _put( BINARY , name );
        _buf.putInt( val.length() );
        _buf.put( val.getType() );
        _buf.put( val.getData() );
    }
    

    protected int putSymbol( String name , Symbol s ){
        return _putString(name, s.getSymbol(), SYMBOL);
    }

    protected int putString(String name, String s) {
        return _putString(name, s, STRING);
    }

    private int _putString( String name , String s, byte type ){
        int start = _buf.position();
        _put( type , name );
        _putValueString( s );
        return _buf.position() - start;
    }

    protected int putObjectId( String name , ObjectId oid ){
        int start = _buf.position();
        _put( OID , name );
        _buf.putInt( oid._time() );
        _buf.putInt( oid._machine() );
        _buf.putInt( oid._inc() );
        return _buf.position() - start;
    }
    
    protected int putDBPointer( String name , String ns , ObjectId oid ){
        int start = _buf.position();
        _put( REF , name );
        
        _putValueString( ns );
        _buf.putInt( oid._time() );
        _buf.putInt( oid._machine() );
        _buf.putInt( oid._inc() );

        return _buf.position() - start;
    }

    protected void putDBRef( String name, DBRefBase ref ){
        _put( OBJECT , name );
        final int sizePos = _buf.position();
        _buf.putInt( 0 );
        
        _putObjectField( "$ref" , ref.getRef() );
        _putObjectField( "$id" , ref.getId() );

        _buf.put( EOO );
        _buf.putInt( sizePos , _buf.position() - sizePos );
    }

    private int putPattern( String name, Pattern p ) {
        int start = _buf.position();
        _put( REGEX , name );
        _put( p.pattern() );
        _put( regexFlags( p.flags() ) );
        return _buf.position() - start;
    }


    // ----------------------------------------------
    
    /**
     * Encodes the type and key.
     * 
     */
    private void _put( byte type , String name ){
        _buf.put( type );
        _put( name );
    }

    void _putValueString( String s ){
        int lenPos = _buf.position();
        _buf.putInt( 0 ); // making space for size
        int strLen = _put( s );
        _buf.putInt( lenPos , strLen );
    }
    
    int _put( String name ){

        _cbuf.position( 0 );
        _cbuf.limit( _cbuf.capacity() );
        _cbuf.append( name );
        
        _cbuf.flip();
        final int start = _buf.position();
        _encoder.encode( _cbuf , _buf , false );

        _buf.put( (byte)0 );

        return _buf.position() - start;
    }

    private final CharBuffer _cbuf = CharBuffer.allocate( MAX_STRING );
    private final CharsetEncoder _encoder = _utf8.newEncoder();
    
    private boolean _flipped = false;
    final ByteBuffer _buf;
}
