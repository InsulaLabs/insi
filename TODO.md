Alias keys are not getting the correct limits
its because the limits are imposed by keyUUID
which is unique to each alias.
We need to scope limits to the datascope to
enforce limits on all items.

- Identify where we create keys initially and ensure that we set the scopes correcrlyt off-cuff for root

- when set limits with --root comes in we can set via alias as its the same data ascope

- when get limits comes in on an alias, give the
limits that are imposed at the datascope level

Updating limits has to happen on the root key 

