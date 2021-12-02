# DBS II Join Framework

This is a framework to simulate different join implementations and their IO costs.

## Build and execute

This is a maven project. You need to have maven installed (https://maven.apache.org/install.html) to be able to build it. To build an executable jar file run `mvn clean package`.

Once building completed, you will find a jar file `join-0.0.1-SNAPSHOT-jar-with-dependencies.jar` in the `target` folder.

To run your first experiment, execute `java -jar target/join-0.0.1-SNAPSHOT-jar-with-dependencies.jar -r1 <INPUT-PATH>/title.basics.sample.tsv  -r2 <INPUT-PATH>/title.principals.sample.tsv  -j1 0 -j2 0` (your sample data should be found under `<INPUT-PATH>`).

This should give you the following output:

```
Input relation sizes (blocks): 2 7
NLJ result: 3563

join.algorithms.HashEquiJoin
IO cost estimate: 27
Result size: 0
Result equals NLJ: false
Real IO cost:0
```

When you are done implementing the hash join, the result of your join implementation should match the NLJ result.

## Data structures

The data is organized as relations, blocks and tuples.

### Relation

A `Relation` is mainly a collection of blocks. You can iterate over its block, as it implements `Iterable<Block>`.

You can declare a whole relation to be an in-memory relation using the constructor `public Relation(BlockManager blockManager, boolean inMemory)`. In this case all that you request via the method `getFreeBlock()` will be in-memory blocks (see **Block states** for their special properties).

### Block

A `Block` contains tuples. Consider instances of this class rather as references to a block.

It depends on the pin state of this block whether its tuples can be accessed or not (see **Block states**). Pinning of blocks is realized by the BlockManager.

### Tuple

A very simple tuple that can only contain Strings, indexed by position.

## Block states

When a block is newly created, it will be in `FRESH` state.
When the freshly created block is pinned, its state will change to `LOADED`.
You need to pin the block before you can read from / write to it.
When the block's content changes, its state will change to `DIRTY`.
When you unpin a block and no other pins for this block exist, its state will change to `UNLOADED`.
Once unpinned, the block needs to be pinned again, before you can access its content.

The following diagram gives an overview of the block states:

![Block states](/doc/BlockStates.png)

The green states indicate that you can access a block in this state.
The orange state transitions indicate that these state transitions will cause IO costs.

You can also create in-memory blocks.
Their states are the same as those for disk blocks, but they do not impose IO costs at any state change.
One important difference between disk blocks and in-memory blocks is that you cannot pin them again, once they reached the `UNLOADED` state (their content is lost, once they reach this state).

## The `Join` interface

The `Join` interface contains two methods:

`void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2, Consumer<Tuple> consumer)`

This method performs the actual join of `relation1` and `relation2`. This is an equi-join with the join-condition that `joinAttribute1` (attribute index) of tuples of `relation1` should equal `joinAttribute2` (attribute index) of tuples of `relation2`. For every resulting tuple, the `consumer` should be called exactly once with the resulting tuple.

`int getIOEstimate(Relation relation1, Relation relation2)`

This method should give an estimate of the IO costs that should be expected when joing the relations `relation1` and `relation2`.

## Helper classes

We provide helper classes for the following two common cases:
* you want to read a relation tuple by tuple. 
* you want to write tuples to a relation.

In both cases you usually would have to make manually sure that you pin the appropriate blocks before you access the blocks and unpin them afterwards.
The two helper classes `join.helper.PinningTupleIterator` and `join.helper.PinningOutputTarget` will take care of the block pinning and unpinning for you.
In some cases, you might need more fine-grained control of the pinning of blocks, where you still have to do the pinning and unpinning manually and cannot rely on those helper classes.
Make sure to call `close()` on both iterators, once you are done reading/writing (otherwise blocks might stay pinned unexpectedly).

