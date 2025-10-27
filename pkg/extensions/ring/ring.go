package ring

/*
	This extension will be made available over HTTP and SSH just like process.
	The root entity can modify/control this extension in the same way

	The ring extension is to "bind" together two individual clusters.

	This means that we can have one 3xCluster and one otehr NxCluster and "bind" them meaning that we will
	replicate the entities on-board.

	This will only sync the entities and access limits across clusters at first. Eventually we may bridge
	events for entities, but data sync is not the target outside of entity records.

	This means the entities kv and blob stores are not duplicated, just that the entities will gain access
	to a new cluster and enjoy the same rights on the external cluster.

	This will be sueful for when we need to spin up an ephemeral server for an instance of something like
	a chat or a game room. We can spin up an external insi cluster, sync the entities, then the players/users/processes
	that are leveraging the entitiy concept can interface immediatly with the resources provided
*/
