package shared

import (
	"log"
	"os"
)

const IncrementalLogFile = "incremental.log"

func WriteIncrementalLog(line string) {
	f, err := os.OpenFile(IncrementalLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("WARNING: Failed to open incremental log file: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		log.Printf("WARNING: Failed to write to incremental log file: %v", err)
	}
}
