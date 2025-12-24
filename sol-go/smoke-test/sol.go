package main

import (
    "fmt"
    "io"
    "net"
    "os"
    "os/signal"
    "runtime"
    "syscall"
	"errors"
)

// handleClient handles a single client connection.
func handleClient(conn net.Conn) {
    // addr is the IP address and port of the client.
    addr := conn.RemoteAddr().String()
    fmt.Printf("[NEW CONNECTION] %s connected.\n", addr)

    // Ensure connection is closed when function exits
    defer func() {
        conn.Close()
        fmt.Printf("[DISCONNECTED] %s disconnected.\n", addr)
    }()

    buffer := make([]byte, 4096)

    for {
        // Read data from the connection
        n, err := conn.Read(buffer)

        if err != nil {
            if err == io.EOF {
                // Client shut down their sending side
                break
            }
            fmt.Printf("[ERROR] Connection error with %s: %v\n", addr, err)
            break
        }

        // Send the data back (echo)
        _, err = conn.Write(buffer[:n])
        if err != nil {
            fmt.Printf("[ERROR] Write error with %s: %v\n", addr, err)
            break
        }
    }
}

func startServer(host string, port string) {
    address := host + ":" + port
    listener, err := net.Listen("tcp", address)
    if err != nil {
        fmt.Printf("[ERROR] Could not start server: %v\n", err)
        return
    }
    // Ensure listener is closed on exit
    defer listener.Close()

    fmt.Printf("[LISTENING] Server is listening on %s\n", address)

    // Handle graceful shutdown
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c // Block until signal is received
        fmt.Println("\n[SHUTTING DOWN] Server stopping...")
        listener.Close() 
    }()

    for {
        conn, err := listener.Accept()
        if err != nil {
            // Check if the error is essentially "Listener Closed"
            if errors.Is(err, net.ErrClosed) {
                // This is expected during shutdown, simply return to stop the loop
                return
            }
            
            fmt.Printf("[ERROR] Accept error: %v\n", err)
            continue
        }

        go handleClient(conn)

        // Adjust active count (approximate)
        fmt.Printf("[ACTIVE CONNECTIONS] %d\n", runtime.NumGoroutine()-2)
    }
}

func main() {
    startServer("0.0.0.0", "65432")
}
