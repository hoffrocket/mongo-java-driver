/**
 *      Copyright (C) 2010 10gen Inc.
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
package com.mongodb.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class StringRangeSet implements Set<String> {
  private final int size;
  public StringRangeSet(int size){
    this.size = size;
  }
  public int size(){
      return size;
  }

  public Iterator<String> iterator(){
      return new Iterator<String>(){
        int index = 0;
        @Override
        public boolean hasNext() {
          return index < size;
        }

        @Override
        public String next() {
          return String.valueOf(index++);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
        
      };
  }
  @Override
  public boolean add(String e) {
    throw new UnsupportedOperationException();
  }
  @Override
  public boolean addAll(Collection<? extends String> c) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean contains(Object o) {
    int t = Integer.valueOf(String.valueOf(o));
    return t >= 0 && t < size;
  }
  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)){
        return false;
      }
    }
    return true;
  }
  @Override
  public boolean isEmpty() {
    return false;
  }
  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }
  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }
  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }
  @Override
  public Object[] toArray() {
    String[] array = new String[size()];
    for (int i = 0; i < size; i++){
      array[i] = String.valueOf(i);
    }
    return array;
  }
  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }
  
}