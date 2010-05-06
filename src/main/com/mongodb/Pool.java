/**
 * 
 */
package com.mongodb;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;


public class Pool<T> {
	private final GenericObjectPool _pool;
	
	public Pool(PoolFactory<T> pf, int minSize, int maxSize) {
		 _pool = new GenericObjectPool(new Pool.PoolFactoryAdapter<T>(pf), maxSize,GenericObjectPool.WHEN_EXHAUSTED_BLOCK, 
					60000, minSize, false, false, 0, 0, 0, false);
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
	
	interface UsePooled<R,T> {
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