# Orchestrator

This orchestrator project is originally copied/reproduced by book "_Build an Orchestrator in Go_" by **Tim Boring**.  

After reading the book and following all steps Tim showed, I got the desire to continue working on this project a bit to 
make a few improvements to get some extra experience with Go by working on a relatively real cases.  


## Next improvements
* Simplify a Manager API to reduce a number of service fields user need to pass (task event and IDs)
* Replace a Worker Rest API with gRPC
* Cover functionality with tests
* Make a stronger abstraction over docker client
* Reduce the complexity of Manager and Worker by introducing an extra layers for business logic
* Publish task events to a message stream
* Make a stronger interface for Store by returning explicit data types
* Introduce an Analytics API
