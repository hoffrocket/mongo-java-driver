// ReadOplog.java

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class ReadOplog {

    public static void main(String[] args) 
        throws Exception {

        Mongo m = new Mongo();
        DB local = m.getDB( "local" );
        
        DBCollection oplog = local.getCollection( "oplog.$main" );
        
        DBObject last = null;
        {
            DBCursor lastCursor = oplog.find().sort( new BasicDBObject( "$natural" , -1 ) ).limit(1);
            if ( ! lastCursor.hasNext() ){
                System.out.println( "no oplog!" );
                return;
            }
            last = lastCursor.next();
        }
        
        BSONTimestamp ts = (BSONTimestamp)last.get("ts");
        System.out.println( "starting point: " + ts );
        
        while ( true ){
            System.out.println( "ts: " + ts );
            DBCursor cursor = oplog.find( new BasicDBObject( "ts" , new BasicDBObject( "$gt" , ts ) ) );
            cursor.addOption( Bytes.QUERYOPTION_TAILABLE );
            cursor.addOption( Bytes.QUERYOPTION_AWAITDATA );
            while ( cursor.hasNext() ){
                DBObject x = cursor.next();
                ts = (BSONTimestamp)x.get("ts");
                System.out.println( "\t" + x );
            }
            
        }
    }
}
