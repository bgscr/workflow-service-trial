# Engineering Guidelines

- Think before coding. State assumptions and surface meaningful tradeoffs. Ask for clarification only when ambiguity would materially change the result.
- Prefer the smallest implementation that fully satisfies the request. Avoid speculative features, unnecessary configurability, and one-off abstractions.
- Make surgical changes. Match the existing style, avoid unrelated refactors, and remove only code made obsolete by the current change.
- Work toward verifiable outcomes. Define concrete success criteria, test proportionately to risk, and continue until the requested result is verified.
- Every changed line should be directly traceable to the user's request.
