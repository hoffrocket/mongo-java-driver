/**
 * 
 */
package com.mongodb.util;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;


public class Pool<T> {
	private final GenericObjectPool _pool;
	
	public Pool(PoolFactory<T> pf, int minSize, int maxSize) {
		 this(pf, minSize, maxSize, 60000);
	}
	
	public Pool(PoolFactory<T> pf, int minSize, int maxSize, int maxWait) {
		//run evictor (shrink) thread every two minutes
		 _pool = new GenericObjectPool(new Pool.PoolFactoryAdapter<T>(pf), maxSize,GenericObjectPool.WHEN_EXHAUSTED_BLOCK, 
					maxWait, minSize, false, false, 60 * 2 * 1000, 0, -1, false);
	}
	
	private final RuntimeException wrap(String message,Exception e) {
		if (RuntimeException.class.isInstance(e)) 
			return (RuntimeException)e;
		else return new RuntimeException(message, e);
	}
	
	@SuppressWarnings("unchecked")
	public <R> R use(UsePooled<R,T> usePooled) {
		T obj;
		
		try {
			obj = (T)_pool.borrowObject();
		} catch (Exception e) {
			throw wrap("Error borrowing object from pool", e);
		}
		
		try {
			return usePooled.use(obj);
		} catch (Exception e) {
			throw wrap("Error using pooled object", e);
		} finally {
			try {
				_pool.returnObject(obj);
			} catch (Exception e) {
				throw wrap("Errror returning object to pool", e);
			}
		}
	}
	
	public interface PoolFactory<T> {
		public T create();
		public void reset(T obj);
	}
	
	public interface UsePooled<R,T> {
		public R use (T thing) throws Exception;
	}
	
	private static class PoolFactoryAdapter<T> extends BasePoolableObjectFactory  {
		private final PoolFactory<T> _pf;
		public PoolFactoryAdapter(PoolFactory<T> pf){
			_pf = pf;
		}
		@Override
		public Object makeObject() throws Exception {
			return _pf.create();
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void passivateObject(Object obj) throws Exception {
			_pf.reset((T)obj);
		}
		
	}
}