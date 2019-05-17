package splitter

import (
	"fmt"
	"log"
	"strings"
)

func Example() {
	// Return blocks no larger than 10 bytes.  Otherwise, use default settings,
	// which includes a Rabin-Karp rolling hash.
	c := Config{Max: 10}

	s := c.New(strings.NewReader("Four score and seven years ago..."))
	if err := s.Split(func(data []byte) error {
		fmt.Println(string(data))
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Output:
	// Four score
	//  and seven
	//  years ago
	// ...
}
