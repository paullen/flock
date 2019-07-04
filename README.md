# Flock

Database migration tool with ETL like workflow 

## Directory Structure
The following tree explains the files contained in this project in a gist

```
- cmd
  - client
    - protos - proto definitions for client and UI conversation
    - main.go - client implementations for client and server conversation
    - serverUI.go - server implementations for client and UI conversation
    - params.go - function to replace named placeholders in .fl file to a driver compliant placeholder
    - verify.go - functions to verify validity of schema and plugins provided by the UI
  - server
    - main.go - server implementations for client and server conversation
- pkg
  - func.go - functions to register user-defined functions into the flock execution for data manipulation of data obtained from source DB
  - insert.go - functions to insert the data into the destination database in batches after manipulation
  - parser.go - a parser implementation for the .fl file
  - plugin.go - function to generate a plugin and lookup function map to be used during data manipulation
  - tables.go - generate the table structure from .fl file
- protos - proto definitions for client and server conversation
- server
  - server.go - server implementations for client and server conversation
  - handlers.go - functions to handle certain tasks in server.go
- sql
  - utility.go - functions that handle certain SQL related tasks(direct interaction with the database).
```

## Working

The architecture of the tool is as depicted in the below figure

```
UI <========> Client <==========> Server
                ||                  ||
                ||                  ||
                ||                  ||
             Source DB         Destination DB
```

The conversations between UI <-> Client and Client <-> Server take place using grpc, grpc-web in case of UI <-> Client.
All the ping and verification actions between the UI and the backend are stateless i.e. the server is dialled each time the UI wants to execute a server-specific task.

The entirity of the backend is exposed to the UI only through Client, Source DB is touched only by Client and Destination DB by Server.
