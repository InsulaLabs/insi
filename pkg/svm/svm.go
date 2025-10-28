package svm

// simple virtual machine

/*


in the insi environment we have a set of operations that any entity can work with. These are

Cache
	set
	get
	delete
	iterate
Value
	set
	get
	delete
	iterate

Events
	Emit to topic
	Subscribe to topic

This defines the environment that the machine si working in. I will use this psuedo code to express the idea:
*/

/*
Takes in a byte array of SVM data and builds a Runtime instance from it.
*/
func BuildRuntimeFromData(data []byte) (*Runtime, *BuildError) {

	// TODO: parse the data into slp object list
	// Iterate over the objects and handle the top-most level
	// objects "process" etc to build the processes.
	// each "process" call will spawn a process builder to construct
	// and validate the process here.

	// The runtime will contain all of the defined processes and
	// have a startup/shutdown body (see example.svm)

	return nil, nil
}
