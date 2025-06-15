This set of tests requires that the project be build into the build directory.

This set of tests uses the insic program to exercise the system. They are a dual-purpose set of tests:

    1) Test the insic program is operating as expected
    2) Integration tests for system components

When running `bash run-all.sh` every aspect except for the tracking of "usage" on a key is tested for the system.
This specific set of tests to monitor/ check usage required deeper level of metrics for sensible testing and are
thus integration tested in the scripted tests under `scripts/tests`