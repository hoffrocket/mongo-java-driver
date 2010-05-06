// DBOutputBuffer.java

package com.mongodb.io;

import org.bson.*;
import org.bson.io.*;
import org.bson.util.*;

import java.io.*;
import java.util.*;

public class DBOutputBuffer extends OutputBuffer {
    
    public static final int BUF_SIZE = 1024 * 16;

    public DBOutputBuffer(){
        reset();
    }

    public void reset(){
        _cur.reset();
        _end.reset();

        for ( int i=0; i<_fromPool.size(); i++ )
            _extra.done( _fromPool.get(i) );
        _fromPool.clear();
    }

    public int getPosition(){
        return _cur.pos();
    }
    
    public void setPosition( int position ){
        _cur.reset( position );
    }
    
    public void seekEnd(){
        _cur.reset( _end );
    }
    
    public void seekStart(){
        _cur.reset();
    }

    
    public int size(){
        return _end.pos();
    }

    public void write(byte[] b){
        write( b , 0 , b.length );
    }

    public void write(byte[] b, int off, int len){
        while ( len > 0 ){
            byte[] bs = _cur();
            int space = Math.min( bs.length - _cur.y , len );
            System.arraycopy( b , off , bs , _cur.y , space );
            _cur.inc( space );
            len -= space;
            off += space;
            _afterWrite();
        }
    }

    public void write(int b){
        byte[] bs = _cur();
        bs[_cur.getAndInc()] = (byte)(b&0xFF);
        _afterWrite();
    }
    
    void _afterWrite(){
        
        if ( _cur.pos() < _end.pos() ){
            // we're in the middle of the total space
            // just need to make sure we're not at the end of a buffer
            if ( _cur.y == BUF_SIZE )
                _cur.nextBuffer();
            return;
        }
        
        _end.reset( _cur );
        
        if ( _end.y < BUF_SIZE )
            return;
        
        _fromPool.add( _extra.get() );
        _end.nextBuffer();
        _cur.reset( _end );
    }
    
    byte[] _cur(){
        return _get( _cur.x );
    }

    byte[] _get( int z ){
        if ( z < 0 )
            return _mine;
        return _fromPool.get(z);
    }

    public int pipe( OutputStream out )
        throws IOException {
        
        int total = 0;
        
        for ( int i=-1; i<_fromPool.size(); i++ ){
            byte[] b = _get( i );
            int amt = _end.len( i );
            out.write( b , 0 , amt );
            total += amt;
        }
        
        return total;
    }

    static class Position {
        Position(){
            reset();
        }
        
        void reset(){
            x = -1;
            y = 0;
        } 
        
        void reset( Position other ){
            x = other.x;
            y = other.y;
        }

        void reset( int pos ){
            x = ( pos / BUF_SIZE ) - 1;
            y = pos % BUF_SIZE;
        }
        
        int pos(){
            return ( ( x + 1 ) * BUF_SIZE ) + y;
        }

        int getAndInc(){
            return y++;
        }

        void inc( int amt ){
            y += amt;
            if ( y > BUF_SIZE )
                throw new IllegalArgumentException( "something is wrong" );
        }
        
        void nextBuffer(){
            if ( y != BUF_SIZE )
                throw new IllegalArgumentException( "broken" );
            x++;
            y = 0;
        }

        int len( int which ){
            if ( which < x )
                return BUF_SIZE;
            return y;
        }

        public String toString(){
            return x + "," + y;
        }

        int x; // which buffer -1 == _mine
        int y; // position in buffer
    }
    
    final byte[] _mine = new byte[BUF_SIZE];
    final List<byte[]> _fromPool = new ArrayList<byte[]>();
    
    private final Position _cur = new Position();
    private final Position _end = new Position();
    
    private static org.bson.util.SimplePool<byte[]> _extra = 
        new org.bson.util.SimplePool<byte[]>( ( 1024 * 1024 * 10 ) / BUF_SIZE ){
        
        protected byte[] createNew(){
            return new byte[BUF_SIZE];
        }

    };
}
