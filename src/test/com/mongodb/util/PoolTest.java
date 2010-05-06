package com.mongodb.util;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.ExpectedExceptions;


import com.mongodb.io.ByteBufferStreamTest;
import com.mongodb.util.Pool.PoolFactory;
import com.mongodb.util.Pool.UsePooled;

public class PoolTest extends TestCase{
	
	AtomicInteger counter = new AtomicInteger();
	
	Pool<Integer> pool = new Pool<Integer>(new PoolFactory<Integer>() {

		@Override
		public Integer create() {
			return counter.incrementAndGet();
		}
		

		@Override
		public void reset(Integer obj) {
			
		}
	}, 2,2,1);
	
	@org.testng.annotations.Test
	void testPoolGetsExpectedObjects(){
		pool.use(new UsePooled<Void, Integer>() {

			@Override
			public Void use(Integer thing) throws Exception {
				assertLess(thing.intValue(),3);
				pool.use(new UsePooled<Void, Integer>() {

					@Override
					public Void use(Integer thing2) throws Exception {
						assertLess(thing2.intValue(),3);
						return null;
					}
				});
				return null;
			}
		});
		pool.use(new UsePooled<Void, Integer>() {

			@Override
			public Void use(Integer thing) throws Exception {
				assertLess(thing.intValue(),3);
				return null;
			}
		});
	}
	
	@org.testng.annotations.Test
	void starvingPoolThrowsException() {
		pool.use(new UsePooled<Void, Integer>() {

			@Override
			public Void use(Integer thing) throws Exception {
				
				pool.use(new UsePooled<Void, Integer>() {

					@Override
					public Void use(Integer thing2) throws Exception {
						try {
							pool.use(new UsePooled<Void, Integer>() {
	
								@Override
								public Void use(Integer thing3) throws Exception {
									assertTrue(false,"Should not get here");
									return null;
								}
							});
							
						} catch (NoSuchElementException e) {
							assertEquals(e.getMessage(), "Timeout waiting for idle object");
						}
						return null;
					}
				});
				return null;
			}
		});
	}
}
