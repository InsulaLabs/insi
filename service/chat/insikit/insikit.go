package insikit

// The toolkit(s) the user can import DURING chat.

/*

	First Targets
	- Clear/Reset Context (the message history) - this means the GetChatResetTool(..) will need to store a callback to kick-off the behaviour
	- CRUD K/V store - will use insiclient and the apikey given during chat (the td modal from the api key) so the user data is segregated to their distributed store

	- CRUD on islands
	   paramount first step - this is how we organize the user's resources which is blocking the abilityu to gen and store docs
	   This means ALL client operations (from the api) need to be made in to its own tool call that the ai can give data to




	[ AFTER INITIAL GENS AND TESTS AND WHATNOT ]
	Second Targets
	- CRUD on objects by calling image gen tools and
	   storing as objects in the object enpoint

	- EVENTING . Whyu not let the user emit an event and have an in-memory session to catch
		events so when chatting the ai can inform the user of the event and what to do next?
*/
