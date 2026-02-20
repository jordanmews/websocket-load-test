# Load-test a websocket server w/ Golang websocket client 
- Configurable # of connections, ramp-up and hold time.
- A simple example server is also included to test against.
- The server periodically sends json messages on a ticker feed.
- The client checks the incoming messages for expected syntax in `TestWsMessageTypeContainsMsg`

## SETUP
From project's root dir:  
`go mod init websocket-load-test`  
`go mod tidy`

## RUN
Run server and client. 
From root project dir, 
```
go run ./server/ .
go run ./client/ .
```
## RUN w/ overrides

For full list of overrides, see the `parseCLI` functions at the top of the client and server main files.
1. [client/main.go](client/main.go)  
   i.e. `go run ./client/ . -connections 100`
2. [server/main.go](server/main.go)  
   i.e. `go run ./server/ . -port 222 -domain ws.example.com`
