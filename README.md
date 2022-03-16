# Tapestry

This project implements Tapestry, an underlying distributed object location and retrieval system (DOLR) which can be used to store and locate objects. This distributed system provides an interface for storing and retrieving key-value pairs.
From an application’s perspective, the application chooses where to store data, rather than allowing the system to choose a node to store the object at.

Tapestry is a decentralized distributed system, where each node serves as both an object store and a router that applications can contact to obtain objects. In a Tapestry network, objects are “published” at nodes, and once an object has been successfully published, it is possible for any other node in the network to find the location at which that object is published.

### High-Level Design Choice
**Node Join:** When a node trying to join the tapestry, the node first finds the shared prefix of ID and multicast to the existing node sharing the prefix. These nodes will add the new node to their routing table. Then new node will get closest neighbors to fill its own routing table.

**Node Leave:** When a node leave the network, the node will notify its leaving and try to transfer the replacement node when traversing its own routing table. Objects stored at leaving node will be redistributed.


### Interesting Improvement

We made some part of our code in goroutine, which will improve the performance of our project.
For the test, we developed some util functions that can make testing much easier.

### Test For Tapestry

We have  put a lot of effort on the tests for this project. Thus, we have written lots of unit tests. Below are the test we have written. The unit tests concentrate on the four main file: `node_init`, `node_core`, `node_exit` and `routing_table` . Beside the unit tests, we also implement some `testing_utils` functions that helping the testing of the project.


***node_init_test.go***

- TestNodeIDAndAddress

  This test tests about the Node ID and Addr function.

- TestMulticast1

  This test tests about Multicast, especially about data transfer in multicast.

- TestMulticast2

  This test tests about Multicast, especially about bad node removed from routing table.

- TestMulticast3

  This test tests about Multicast, especially about bad node not in neighbor.

- TestAddRoute1

  This test tests about AddRoute, especially about adding route to routing table.

- TestAddRoute2

  This test tests about AddRoute, especially about replacing a node in routing table.

- TestAddRoute3

  This test tests about AddRoute, especially about not replacing a node in routing table, because new node is further.


***node_core_test.go***

- TestNodeGetStoreRemove

  This test tests about Node Get Store Remove, especially when these operation between remote nodes.

- TestFindRootOnRemoteNode1

  This test tests about FindRoot for deleted node, which will return the following node in the routing table.

- TestFindRootOnRemoteNode1_BadNode

  This test tests about FindRootOnRemoteNode, especially when find root with one better bad node, the result should returns self.

- TestFindRootOnRemoteNode2_BadNode

  This test tests about FindRootOnRemoteNode, especially when find root on a bad node.

- TestPublish

  This test tests about Publish which registers object on the correct root node


***node_exit_test.go***

- TestLeave1_SafeLeave

  This test tests about node leave the tapestry, especially when leaving in a graceful way

- TestLeave2_UnsafeLeave

  This test tests about node leave the tapestry, especially when leaving in an unsafe way

- TestNotifyLeave1_WithoutReplacement

  This test tests about NotifyLeave, especially when leaving without a replacement node

- TestNotifyLeave2_Replacement

  This test tests about NotifyLeave, especially when leaving with a replacement node

- TestNotifyLeave2_WithReplacement

  This test tests about Leave, especially when gracefully leaving with a replacement node


***routing_table_test.go***

- TestRoutingTable_Add

  This test tests about Add and Remove node in routing table, especially when function called on remote node

### Test Coverage

**node_init.go: 85.5%**

**node_core.go: 85.1%**

**node_exit.go: 95.7%**

**routing_table.go: 96.5%**

### Reference

https://sites.cs.ucsb.edu/~ravenben/classes/papers/tapestry-jsac04.pdf