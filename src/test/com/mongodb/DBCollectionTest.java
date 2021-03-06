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

import java.io.*;
import java.util.*;

import org.testng.annotations.*;

import com.mongodb.util.*;

public class DBCollectionTest extends TestCase {

    public DBCollectionTest()
        throws IOException , MongoException {
        super();
	cleanupMongo = new Mongo( "127.0.0.1" );
	cleanupDB = "com_mongodb_unittest_DBCollectionTest";
        _db = cleanupMongo.getDB( cleanupDB );
    }
    @Test(groups = {"basic"})
    public void testMultiInsert() {
        DBCollection c = _db.getCollection("testmultiinsert");
        c.drop();
        
        DBObject obj = c.findOne();
        assertEquals(obj, null);

        DBObject inserted1 = BasicDBObjectBuilder.start().add("x",1).add("y",2).get();
        DBObject inserted2 = BasicDBObjectBuilder.start().add("x",3).add("y",3).get();
        c.insert(inserted1,inserted2);
        c.insert(new DBObject[] {inserted1,inserted2});
    }
    @Test(groups = {"basic"})
    public void testFindOne() {
        DBCollection c = _db.getCollection("test");
        c.drop();
        
        DBObject obj = c.findOne();
        assertEquals(obj, null);

        DBObject inserted = BasicDBObjectBuilder.start().add("x",1).add("y",2).get();
        c.insert(inserted);
        c.insert(BasicDBObjectBuilder.start().add("_id", 123).add("x",2).add("z",2).get());

        obj = c.findOne(123);
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.get("z"), 2);

        obj = c.findOne(123, new BasicDBObject("x", 1));
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.containsField("z"), false);

        obj = c.findOne(new BasicDBObject("x", 1));
        assertEquals(obj.get("x"), 1);
        assertEquals(obj.get("y"), 2);

        obj = c.findOne(new BasicDBObject("x", 1), new BasicDBObject("y", 1));
        assertEquals(obj.containsField("x"), false);
        assertEquals(obj.get("y"), 2);
    }
    
    @Test
    public void testDropIndex(){
        DBCollection c = _db.getCollection( "dropindex1" );
        c.drop();

        c.save( new BasicDBObject( "x" , 1 ) );
        assertEquals( 1 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.dropIndexes();
        assertEquals( 1 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "y" , 1 ) );
        assertEquals( 3 , c.getIndexInfo().size() );
        
        c.dropIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );
        
    }

    @Test
    public void testGenIndexName(){
        BasicDBObject o = new BasicDBObject();
        o.put( "x" , 1 );
        assertEquals("x_1", DBCollection.genIndexName(o));

        o.put( "x" , "1" );
        assertEquals("x_1", DBCollection.genIndexName(o));

        o.put( "x" , "2d" );
        assertEquals("x_2d", DBCollection.genIndexName(o));

        o.put( "y" , -1 );
        assertEquals("x_2d_y_-1", DBCollection.genIndexName(o));

        o.put( "x" , 1 );
        o.put( "y" , 1 );
        o.put( "a" , 1 );
        assertEquals( "x_1_y_1_a_1" , DBCollection.genIndexName(o) );

        o = new BasicDBObject();
        o.put( "z" , 1 );
        o.put( "a" , 1 );
        assertEquals( "z_1_a_1" , DBCollection.genIndexName(o) );
    }

    @Test
    public void testDistinct(){
        DBCollection c = _db.getCollection( "distinct1" );
        c.drop();

        for ( int i=0; i<100; i++ ){
            BasicDBObject o = new BasicDBObject();
            o.put( "_id" , i );
            o.put( "x" , i % 10 );
            c.save( o );
        }

        List l = c.distinct( "x" );
        assertEquals( 10 , l.size() );

        l = c.distinct( "x" , new BasicDBObject( "_id" , new BasicDBObject( "$gt" , 95 ) ) );
        assertEquals( 4 , l.size() );

    }

    @Test
    public void testEnsureIndex(){
        DBCollection c = _db.getCollection( "ensureIndex1" );
        c.drop();
        
        c.save( new BasicDBObject( "x" , 1 ) );
        assertEquals( 1 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) , new BasicDBObject( "unique" , true ) );
        assertEquals( 2 , c.getIndexInfo().size() );
        assertEquals( Boolean.TRUE , c.getIndexInfo().get(1).get( "unique" ) );
    }

    @Test
    public void testIndexExceptions(){
        DBCollection c = _db.getCollection( "indexExceptions" );
        c.drop();

        c.insert( new BasicDBObject( "x" , 1 ) );
        c.insert( new BasicDBObject( "x" , 1 ) );
        
        c.ensureIndex( new BasicDBObject( "y" , 1 ) );
        c.resetIndexCache();
        c.ensureIndex( new BasicDBObject( "y" , 1 ) ); // make sure this doesn't throw
        c.resetIndexCache();
        
        Exception failed = null;
        try {
            c.ensureIndex( new BasicDBObject( "x" , 1 ) , new BasicDBObject( "unique" , true ) );
        }
        catch ( MongoException.DuplicateKey e ){
            failed = e;
        }
        assertNotNull( failed );

    }


    final DB _db;

    public static void main( String args[] )
        throws Exception {
        (new DBCollectionTest()).runConsole();
    }

}
