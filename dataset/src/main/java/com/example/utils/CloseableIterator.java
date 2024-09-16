package com.example.utils;

import java.util.Iterator;

public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {

}
