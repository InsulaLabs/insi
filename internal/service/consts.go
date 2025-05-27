/*
	For authorization purposes we need a way to store in teh database a set of
	encrypted API keys that nobody else can just.. iterate.

	So: I assembled the simple prefix method of prefixing the api keys with the
	auth token (internally)

	This means that nobody can iterate the keys unles they have the auth token
	which is the _root_ auth token, meaning they would have permission.

	This, in-addition to only permitting prfix iteration of an empty prefix to
	the root account means that the secrets can't be found

	The entity tag is so we can easily iterate the api keys related to a specific
	entity, and the PTSK is used for token lookup and validation.
*/

package service

const (
	EntityRoot = "root"
)
