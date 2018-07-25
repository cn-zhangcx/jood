package com.github.dxee.woow.kafka.consumer;


public interface RetryDelayer {

    // returns true if we should still retrying
    boolean delay();

    // reset the previous attempt if we have a new retry-case
    void reset();
}
