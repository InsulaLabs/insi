package models

/*

  1) upload/set large object on a single node.
  2) event-out to other nodes to sync the object, giving them the info to get the file from this server
  3) other nodes download the file from this server

  complete

  each node should have their own manifest of:

  1) The object UUID, HASH, original upload data, original node uploaded-to and the UUID of who owns the object



  Should consider setting up an honest udp p2p connection between nodes for object sharing.
  Realistically we could then split the admin and raft http mounts to different servers so we
  can mount them privately to the nodes, and only expose the entity-level operations to the public.

  that + the limiting of ip input to the cluster should be enough to make it secure.
*/
