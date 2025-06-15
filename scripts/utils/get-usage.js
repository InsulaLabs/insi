var usage = admin.getLimits().usage;
var max_limits = admin.getLimits().max_limits;
test.Yay("Usage: " + JSON.stringify(usage));
test.Yay("Max Limits: " + JSON.stringify(max_limits));

0;