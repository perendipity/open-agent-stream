# Privacy Posture

Privacy is enforced in the pipeline, not delegated to a UI.

Policy can act at three stages:

1. before local sink delivery
2. before canonical event delivery
3. before raw delivery to external sinks

The default stance is local retention with explicit per-sink release.

