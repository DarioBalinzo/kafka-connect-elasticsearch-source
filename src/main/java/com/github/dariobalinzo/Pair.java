package com.github.dariobalinzo;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Pair<L, R> {
    private final L l;
    private final R r;


    public Pair(@JsonProperty("l") L l, @JsonProperty("r") R r) {
        this.l = l;
        this.r = r;
    }

    public L getL() {
        return l;
    }

    public R getR() {
        return r;
    }
}
