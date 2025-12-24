package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "math"
    "net"
)

// Request defines the expected structure of client data.
type Request struct {
    Method *string  `json:"method"` 
    Number *float64 `json:"number"`
}

// Response defines the structure we send back.
type Response struct {
    Method string `json:"method"`
    Prime  bool   `json:"prime"`
}

// isPrime checks if the number is a valid prime integer.
func isPrime(n float64) bool {
    // Check if it is an integer (e.g., 5.0 is okay, 5.5 is not)
    if n != math.Trunc(n) {
        return false
    }

    // Convert to integer for primality test
    i := int64(n)

    if i <= 1 {
        return false
    }
    if i <= 3 {
        return true
    }
    if i%2 == 0 || i%3 == 0 {
        return false
    }

    for k := int64(5); k*k <= i; k += 6 {
        if i%k == 0 || i%(k+2) == 0 {
            return false
        }
    }
    return true
}

func handleClient(conn net.Conn) {
    // Ensure connection closes when this function returns
    defer conn.Close()

    fmt.Printf("[NEW CONNECTION] %s connected.\n", conn.RemoteAddr())

    // bufio.Scanner handles the buffering and splitting by '\n' automatically
    scanner := bufio.NewScanner(conn)

    for scanner.Scan() {
        // scanner.Bytes() gets the raw line excluding the newline char
        line := scanner.Bytes()

        // 1. Parse JSON
        var req Request
        if err := json.Unmarshal(line, &req); err != nil {
            conn.Write([]byte("malformed\n"))
            return // Disconnect immediately
        }

        // Check for missing fields (nil) or incorrect method
        if req.Method == nil || *req.Method != "isPrime" || req.Number == nil {
            conn.Write([]byte("malformed\n"))
            return // Disconnect immediately
        }

        isP := isPrime(*req.Number)

        // 4. Send Response
        resp := Response{
            Method: "isPrime",
            Prime:  isP,
        }

        respBytes, err := json.Marshal(resp)
        if err != nil {
            fmt.Printf("[ERROR] marshalling response: %v\n", err)
            return
        }

        // Append newline as required by protocol and write
        conn.Write(append(respBytes, '\n'))
    }

    if err := scanner.Err(); err != nil {
        fmt.Printf("[ERROR] connection error with %s: %v\n", conn.RemoteAddr(), err)
    } else {
        fmt.Printf("[DISCONNECTED] %s disconnected.\n", conn.RemoteAddr())
    }
}

func main() {
    port := ":65432"
    listener, err := net.Listen("tcp", port)
    if err != nil {
        fmt.Println("[ERROR] Could not start server:", err)
        return
    }
    defer listener.Close()

    fmt.Println("[LISTENING] Server is listening on", port)

    for {
        // Accept blocks until a new client connects
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("[ERROR] accepting connection:", err)
            continue
        }

        go handleClient(conn)
    }
}
