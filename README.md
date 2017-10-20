# flapper

A work in progress.

Clojure's core.async facilities are a powerful tool to implement processing 'pipelines' in the style of CSP. But building understandable, robust and transparent pipelines to be used in production is tricky. Flapper helps by:

* Wrapping your simple functions with core.async channels for you. You can write functions that are uncluttered as possible with channel inputs and outputs and test them at the repl or in unit tests. Flapper will wrap them in the necessary async channels to make them work in the pipeline.
* Wrapping your functions in error handling and monitoring code.
* Allowing you to declartively set timeouts on individual stages of the pipeline.
* Declaratively construct a pipeline. Concurrency, timeouts, spec checking, etc can be set in policy instead of inside your functions.

## Usage

TODO

## License

Copyright © 2017 Matt Keller

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
