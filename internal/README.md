# Internal Packages

The app/ defines the SSH app that runs on the node.
    chat
    editor

Are packages that supply ssh applications that are "loaded onto" the ssh "app" when invoked by the user.
The difference between an "ssh app" and "extensions" are that "extensions" are meant to be used cluster-wide
and available outside and without relating to ssh services. The Extensions have a "command" interface that we leverage
for ssh access, but their existence is considered exclusive to the presence of an ssh server, while the ssh apps are not.

