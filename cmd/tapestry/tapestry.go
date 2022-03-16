/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: implements a command line interface for running a Tapestry node.
 */

package main

import (
	"flag"
	"fmt"
	"strings"
	tapestry "tapestry/pkg"
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
	"github.com/abiosoft/ishell"
)

func init() {
	// Uncomment for xtrace
	// err := xtr.Connect(xtr.DefaultServerString)
	// if err != nil {
	// 	fmt.Println("Failed to connect to XTrace server. Ignoring trace logging.")
	// }
	return
}

func main() {
	// Uncomment for xtrace
	// defer xtr.Disconnect()
	var port int
	var addr string
	var debug bool

	flag.IntVar(&port, "port", 0, "The server port to bind to. Defaults to a random port.")
	flag.IntVar(&port, "p", 0, "The server port to bind to. Defaults to a random port. (shorthand)")

	flag.StringVar(&addr, "connect", "", "An existing node to connect to. If left blank, does not attempt to connect to another node.")
	flag.StringVar(&addr, "c", "", "An existing node to connect to. If left blank, does not attempt to connect to another node.  (shorthand)")

	flag.BoolVar(&debug, "debug", false, "Turn on debug message printing.")
	flag.BoolVar(&debug, "d", false, "Turn on debug message printing. (shorthand)")

	flag.Parse()

	tapestry.SetDebug(debug)

	switch {
	case port != 0 && addr != "":
		tapestry.Out.Printf("Starting a node on port %v and connecting to %v\n", port, addr)
	case port != 0:
		tapestry.Out.Printf("Starting a standalone node on port %v\n", port)
	case addr != "":
		tapestry.Out.Printf("Starting a node on a random port and connecting to %v\n", addr)
	default:
		tapestry.Out.Printf("Starting a standalone node on a random port\n")
	}

	t, err := tapestry.Start(tapestry.RandomID(), port, addr)

	if err != nil {
		fmt.Printf("Error starting tapestry node: %v\n", err)
		return
	}

	tapestry.Out.Printf("Successfully started: %v\n", t)

	// Kick off CLI, await exit
	CLI(t)

	tapestry.Out.Println("Closing tapestry")
}

// CLI starts the CLI
func CLI(t *tapestry.Node) {
	shell := ishell.New()
	printHelp(shell)

	shell.AddCmd(&ishell.Cmd{
		Name: "help",
		Func: func(c *ishell.Context) {
			printHelp(shell)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "table",
		Func: func(c *ishell.Context) {
			c.Println(t.RoutingTableToString())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "backpointers",
		Func: func(c *ishell.Context) {
			c.Println(t.BackpointersToString())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "replicas",
		Func: func(c *ishell.Context) {
			c.Println(t.LocationMapToString())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "leave",
		Func: func(c *ishell.Context) {
			t.Leave()
			c.Println("Left the tapestry gracefully")
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "kill",
		Func: func(c *ishell.Context) {
			t.Kill()
			c.Println("Left the tapestry abruptly")
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "list",
		Func: func(c *ishell.Context) {
			c.Println(t.BlobStoreToString())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "put",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 2 {
				c.Println("USAGE: put <key> <value>")
				return
			}
			err := t.Store(c.Args[0], []byte(c.Args[1]))
			if err != nil {
				c.Err(err)
				return
			}
			c.Printf("Successfully stored value (%v) at key (%v)\n", c.Args[1], c.Args[0])
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "lookup",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Println("USAGE: lookup <key>")
				return
			}
			replicas, err := t.Lookup(c.Args[0])
			if err != nil {
				c.Err(err)
				return
			}
			c.Printf("%v: %v\n", c.Args[0], replicas)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "get",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Println("USAGE: get <key>")
				return
			}
			bytes, err := t.Get(c.Args[0])
			if err != nil {
				c.Err(err)
				return
			}
			c.Printf("%v: %v\n", c.Args[0], string(bytes))
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "remove",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Println("USAGE: remove <key>")
				return
			}
			exists := t.Remove(c.Args[0])
			if !exists {
				c.Printf("This node is not advertising %v\n", c.Args[0])
			} else {
				c.Printf("Successfully removed %v\n", c.Args[0])
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "debug",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Println("USAGE: debug <on|off>")
				return
			}
			debugstate := strings.ToLower(c.Args[0])
			switch debugstate {
			case "on", "true":
				{
					tapestry.SetDebug(true)
					c.Println("Debug turned on")
				}
			case "off", "false":
				{
					tapestry.SetDebug(false)
					c.Println("Debug turned off")
				}
			default:
				{
					c.Err(fmt.Errorf("Unknown debug state %s. Expect on or off", debugstate))
				}
			}
		},
	})

	shell.Run()
}

func printHelp(shell *ishell.Shell) {
	shell.Println("Commands:")
	shell.Println(" - help                    Prints this help message")
	shell.Println(" - table                   Prints this node's routing table")
	shell.Println(" - backpointers            Prints this node's backpointers")
	shell.Println(" - replicas                Prints the advertised objects that are registered to this node")
	shell.Println("")
	shell.Println(" - put <key> <value>       Stores the provided key-value pair on the local node and advertises the key to the tapestry")
	shell.Println(" - lookup <key>            Looks up the specified key in the tapestry and prints its location")
	shell.Println(" - get <key>               Looks up the specified key in the tapestry, then fetches the value from one of the replicas")
	shell.Println(" - remove <key>            Remove the specified key from the tapestry")
	shell.Println(" - list                    List the blobs being stored and advertised by the local node")
	shell.Println("")
	shell.Println(" - debug on|off            Turn debug on or off.  Off by default")
	shell.Println("")
	shell.Println(" - leave                   Instructs the local node to gracefully leave the tapestry")
	shell.Println(" - kill                    Leaves the tapestry without graceful exit")
	shell.Println(" - exit                    Quit this CLI")
}
