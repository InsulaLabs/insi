# The Performance Trade-Off I Didn't Know I Was Making

Building a distributed system from scratch is one thing. Trusting it is another entirely. My crusty suite of bash and JavaScript tests proved that the features I built didn't immediately fall over, which is a low but necessary bar to clear. But before I could even think about letting real users touch this thing, I needed to know how it behaved under actual pressure. It's time to stop poking it with a stick and start hitting it with a hammer.

## The First Slap 👋

To that end, I built a dedicated stress-testing tool I affectionately call `fwit-t` ("fuck with it - test"). Its job is simple: spin up a configurable number of concurrent clients, or "entities," and have them absolutely hammer the API with a random mix of operations for a set duration. For the first real run, I pointed it at my production-ish cluster, configured it for 50 concurrent entities, and let it rip for 10 minutes.

And... it worked. Beautifully. The cluster hummed along, handling over 135,000 operations without breaking a sweat. Memory and CPU usage were stable and reasonable. The eventing system, which I was particularly nervous about, showed zero message loss. It was a solid win.

But digging into the metrics revealed something strange. A tale of two latencies. While writes were fairly consistent, reads (`GET` requests) were clearly split into two camps:
*   **Camp A:** A bunch of reads were screaming fast, averaging around **45ms**.
*   **Camp B:** Another bunch were consistently slower, averaging **95ms**.

Why the split? It came down to my client's connection strategy. To spread the load, a client would pick a random node from the cluster for its requests. If it happened to pick the leader, it got a fast response. If it picked a follower, the follower would have to forward the request to the leader, adding an extra network hop and nearly doubling the latency. The system was working as designed, but the user experience would be a coin toss. Not ideal.

## An Accidental Optimization

This is where the design of the write path provided a solution. The client already knew how to handle writes to followers: it would get a redirect to the leader and automatically resubmit the request there. It was a self-correcting mechanism.

Which led to a simple question: If the client is smart enough to *find* the leader, why does it immediately forget where it is?

The fix was obvious in hindsight. I made the client "sticky." After it receives its first redirect, it latches onto that leader's address and sends all subsequent requests from that client instance directly there. It was a small code change, and I tucked it behind a `DisableLeaderStickiness` flag, just in case this brilliant idea turned out to be a stupid one.

## The Second Slap (and the Trade-Off it Revealed) 🧐

With the new "sticky" client in hand, I ran the exact same 10-minute test.

The good news hit me first: the bimodal latency was completely gone. Every single client, regardless of which node it connected to initially, quickly found the leader and stayed there. The read-latency graph was now a flat, consistent line. A resounding success.

But success came with a price. A trade-off I hadn't explicitly sought, but one that the system revealed to me.
*   **Read latency** was now a consistent **~128ms**.
*   **Write latency** had increased from **~235ms** to **~310ms**.
*   **Overall throughput** dropped by about **15%**.

At first glance, this looks like a regression. Slower reads? Slower writes? Less throughput? What gives?

The answer is that by making the client "smarter," I funneled the entire workload of 50 concurrent entities onto a single leader node. Before, that load was spread across the whole cluster. The ~45ms reads were fast because the leader was only handling a fraction of the total traffic. Now, the leader was handling *everything*, and it was sweating.

This isn't a bug; it's a classic, fascinating, and incredibly valuable engineering trade-off.

For a user-facing website, consistency is almost always more important than raw, peak performance. A predictable 128ms response time for every user is a much better experience than a lottery where some get 45ms and others get a sluggish 95ms. I had traded a bit of overall throughput for a massive gain in predictability.

This is the kind of insight that no amount of unit testing or isolated feature validation will ever give you. It's the emergent behavior that only appears when you throw the entire, complex system into a cage and make it fight. And it's why, after all this, I'm starting to think that maybe, just maybe, this thing is ready for the real world.
